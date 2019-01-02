package exampletasks

import (
	"fmt"
	"time"

	"github.com/RichardKnop/machinery/v1/log"
)

// LongRunningTask ...
func LongRunningTask() error {
	log.INFO.Print("Long running task started")
	for i := 0; i < 10; i++ {
		log.INFO.Print(10 - i)
		time.Sleep(1 * time.Second)
	}
	log.INFO.Print("Long running task finished")
	return nil
}

// 环境初始化
func InitSetup(ip string) (string, error) {
	return ip + "环境初始化完毕", nil
}

// 环境初始化
func InstallSoft(ip, soft string) (string, error) {
	return ip + "上安装" + soft + "完成", nil
}

// 环境检查
func Check(ip, soft string) (string, error) {
	fmt.Println(ip, soft)
	return "环境 ok\n进程 ok", nil
}
