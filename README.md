# Lab1_MapReduce
2022版本的Lab1

那个讲课大佬严肃声明了不准公开代码，尤其点名了中国人（但是起手做这玩意真的迷茫，所以没放完整代码，仅供参考）

这个lab特点就是，是一个多端交互处理任务的“多机协同”，但是多个进程只用rpc通讯，而且文件系统都在本地，不涉及文件一致性的问题，有限入门难度。

为了方便debug拆出来两个小工程，将testwork中coordinator和rpc、testwork2中的worker复制到mit的源文件，运行bash test即可。

**主要问题**

参考https://zhuanlan.zhihu.com/p/425093684

- 一开始可以从mr/worker.go的 Worker()方法做，发送rpc给coordinator请求任务，然后coordinator分配任务，然后worker读文件并且处理。
- map reduce函数都是通过go插件装载 (.so文件)
- 中间文件命名 mr-X-Y X是map任务号，y是reduce任务号
- worker的map方法用json存储中间kv对，reduce再读回来。（假设设定为n个mapjob，m个reducejob。即map生成的mr-X-Y,x是map任务号，Y是0 ~ m-1，一个mapworker生成m个文件，总共n*m个文件。reduceworker i 将mr-X-Y中Y等于 i的文件全部reduce生成mr-out-i ）。
- worker的map可以用 worker.go里面的ihash(key)得到特定key的reduce任务号
- worker有时候需要等待,比如当map任务都分发出去了,有的worker完成后又来申请任务，此时还有map未完成,reduce不能开始，这个worker需要等待下
- 如果任务重试机制，记得不要生成重复任务
- mrapps/crash.go 随机干掉map reduce,看crash.go的代码是有一定几率让worker直接退出或者长时间延迟,可以用来测试恢复功能。这个逻辑是整合在map reduce函数里面的，注意worker被干掉时候任务已经拿到手了。

**解决方案**

1. 有限状态机（？）

   每一个job有undistributed、waiting、ok三种状态，只能按照u->w、w->o、w->u(reset job)三种状态变换，避免出现job重复分发等问题

2. 临界区

   Map[int] Job 和 heap。Map储存了所有的job包括mapjob和reducejob、heap储存了所有已经分发的任务，按照时间从小到大排列。阻塞的chan不能写在临界区里，会出问题，用select来访问chan。

3. coordinator起四个协程：两个putJob、checkTime、handleReport。

   a：设置两个chan，用来广播所有协程mapAllDone、allJobJDone两个事件。

   b：一个putJob分发mapJob，另一个分发reduceJob。分发reduceJob的putJob协程会等待mapAllDone事件。并且putJob协程会反复遍历map确认是否有上述两个事件，发生了则close chan。

   c：本文的worker不分reduce和map，即rpc调用收到的reply任务是啥就做啥，题目要求worker会自己退出，即allJobDone发生事件时退出worker。

   d：test_crash会随机kill worker进程，用checkTime检查heap中的超时任务，并reset。（这里worker任务做了一部分的话，没有正常report，则coordinator的handleReport没收到的话，依旧reset，因为worker中doMapWork和doReduceWork创建文件用的os.create会清空同名文件，直接覆盖掉了）

讲道理以前看的并发模式忘求了，这个写的性能很垃圾，至少通过测试了。

分布式系统要考虑的故障问题这里倒是体现了很多，性能扩展问题没咋涉及，这lab的性能瓶颈是coordinator处理每个job生命周期的效率吧。这里coordinator只用一个jobchan分发所有任务，一个reportchan收集worker的report。

附加题没做，还不会go net编程。而且处理分布在多机上的map文件交给某一机的reduce worker这问题大发了。。。

说话说一半，另一半非得在括号里说的人都ybb！