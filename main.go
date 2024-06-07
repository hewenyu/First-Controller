package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

// buildConfig 函数基于给定的 kubeconfig 构建一个 Kubernetes 配置对象，如果 kubeconfig 为空，则使用集群内配置。
func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, err
		}
		return cfg, nil
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func main() {
	klog.InitFlags(nil)

	var kubeconfig string
	var leaseLockName string
	var leaseLockNamespace string
	var id string

	flag.StringVar(&kubeconfig, "kubeconfig", "", "kubeconfig 文件的绝对路径")
	flag.StringVar(&id, "id", uuid.New().String(), "持有者ID身份")
	flag.StringVar(&leaseLockName, "lease-lock-name", "", "租用锁资源名称")
	flag.StringVar(&leaseLockNamespace, "lease-lock-namespace", "", "租用锁资源命名空间")
	flag.Parse()

	if leaseLockName == "" {
		klog.Fatal("无法获取租用锁资源名称（缺少租用锁名称标志）.")
	}
	if leaseLockNamespace == "" {
		klog.Fatal("无法获取租约锁资源命名空间（缺少 lease-lock-namespace 标志）.")
	}

	// lease lock 的名字和命名空间、持有者标识等
	// 分布式系统通常需要租约（Lease）；租约提供了一种机制来锁定共享资源并协调集合成员之间的活动。 在 Kubernetes 中，租约概念表示为 coordination.k8s.io API 组中的 Lease 对象， 常用于类似节点心跳和组件级领导者选举等系统核心能力
	config, err := buildConfig(kubeconfig)
	if err != nil {
		klog.Fatal(err)
	}
	client := clientset.NewForConfigOrDie(config)

	run := func(ctx context.Context) {
		// 在这里完成你的控制器循环
		klog.Info("Controller loop...")

		select {}
	}

	// 创建一个可取消(context.WithCancel)的Go context，用于通知选举代码何时适当放弃领导者位置
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 注册一个用于监听中断信号(SIGTERM)的Go例程，一旦接收到中断信号，就取消Context并退出程序。
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		klog.Info("接收到终止信号")
		cancel()
	}()

	// 定义一个租约锁对象(LeaseLock)。这个租约锁将在Kubernetes集群中用于进行领导者选举。
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseLockName,
			Namespace: leaseLockNamespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}

	// 运行领导者选举。LeaderElectionConfig中定义了如何获取和释放锁，以及一旦自身获得或丢失领导权时应该执行的操作。如果领导者身份改变，也会通过回调函数通知。
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock: lock,
		// IMPORTANT: you MUST ensure that any code you have that
		// is protected by the lease must terminate **before**
		// you call cancel. Otherwise, you could have a background
		// loop still running and another process could
		// get elected before your background loop finished, violating
		// the stated goal of the lease.
		ReleaseOnCancel: true,
		LeaseDuration:   60 * time.Second,
		RenewDeadline:   15 * time.Second,
		RetryPeriod:     5 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				// we're notified when we start - this is where you would
				// usually put your code
				run(ctx)
			},
			OnStoppedLeading: func() {
				// we can do cleanup here
				klog.Infof("leader lost: %s", id)
				os.Exit(0)
			},
			OnNewLeader: func(identity string) {
				// we're notified when new leader elected
				if identity == id {
					// I just got the lock
					return
				}
				klog.Infof("new leader elected: %s", identity)
			},
		},
	})
}
