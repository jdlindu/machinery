package exampletasks

import (
	"fmt"
	"math"
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

func Echo(second int, arg string) (string, error) {
	time.Sleep(time.Duration(second) * time.Second)
	fmt.Println(arg)
	if arg == "query" {
		t := time.Now().Unix()
		if math.Mod(float64(t), 5) == 0 {
			return "done", nil
		} else {
			return "", fmt.Errorf("task not finish yet")
		}
	}
	return arg, nil
}
