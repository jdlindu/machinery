package main

import (
	"fmt"
	"git.code.oa.com/storage-ops/golib/common/utils"
	"os"
	"time"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/urfave/cli"

	"github.com/RichardKnop/machinery/myexample/tasks"
)

var (
	app        *cli.App
	configPath string
)

func init() {
	// Initialise a CLI app
	app = cli.NewApp()
	app.Name = "machinery"
	app.Usage = "machinery worker and send example tasks with machinery send"
	app.Author = "Richard Knop"
	app.Email = "risoknop@gmail.com"
	app.Version = "0.0.0"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "c",
			Value:       "",
			Destination: &configPath,
			Usage:       "Path to a configuration file",
		},
	}
}

func main() {
	// Set the CLI app commands
	app.Commands = []cli.Command{
		{
			Name:  "worker",
			Usage: "launch machinery worker",
			Action: func(c *cli.Context) error {
				if err := worker(); err != nil {
					return cli.NewExitError(err.Error(), 1)
				}
				return nil
			},
		},
		{
			Name:  "send",
			Usage: "send example tasks ",
			Action: func(c *cli.Context) error {
				if err := send(); err != nil {
					return cli.NewExitError(err.Error(), 1)
				}
				return nil
			},
		},
		{
			Name:  "query",
			Usage: "query chain task",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "task",
					Value: "",
					Usage: "chain task id",
				},
			},
			Action: func(c *cli.Context) error {
				taskID := c.String("task")
				if err := queryTask(taskID); err != nil {
					return cli.NewExitError(err.Error(), 1)
				}
				return nil
			},
		},
		{
			Name:  "querys",
			Usage: "query group of chain tasks",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "task",
					Value: "",
					Usage: "chain tasks group id",
				},
			},
			Action: func(c *cli.Context) error {
				taskID := c.String("task")
				if err := queryGroupTask(taskID); err != nil {
					return cli.NewExitError(err.Error(), 1)
				}
				return nil
			},
		},
		{
			Name:  "queryTaskByID",
			Usage: "query chain task",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "task",
					Value: "",
					Usage: "chain tasks group id",
				},
			},
			Action: func(c *cli.Context) error {
				taskID := c.String("task")
				if err := queryTaskByID(taskID); err != nil {
					return cli.NewExitError(err.Error(), 1)
				}
				return nil
			},
		},
		{
			Name:  "queryChainTaskByID",
			Usage: "query chain task",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "task",
					Value: "",
					Usage: "chain tasks group id",
				},
			},
			Action: func(c *cli.Context) error {
				taskID := c.String("task")
				if err := queryChainTask(taskID); err != nil {
					return cli.NewExitError(err.Error(), 1)
				}
				return nil
			},
		},
		{
			Name:  "queryTasksByTag",
			Usage: "query group of chain tasks",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "task",
					Value: "",
					Usage: "chain tasks group id",
				},
			},
			Action: func(c *cli.Context) error {
				if err := queryTasks(); err != nil {
					return cli.NewExitError(err.Error(), 1)
				}
				return nil
			},
		},
		{
			Name:  "cancel",
			Usage: "cancel chain task",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "task",
					Value: "",
					Usage: "chain task id",
				},
			},
			Action: func(c *cli.Context) error {
				taskID := c.String("task")
				if err := cancel(taskID); err != nil {
					return cli.NewExitError(err.Error(), 1)
				}
				return nil
			},
		},
		{
			Name:  "skip",
			Usage: "skip fail chain task step",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "task",
					Value: "",
					Usage: "chain task id",
				},
			},
			Action: func(c *cli.Context) error {
				taskID := c.String("task")
				if err := skip(taskID); err != nil {
					return cli.NewExitError(err.Error(), 1)
				}
				return nil
			},
		},
		{
			Name:  "retry",
			Usage: "retry chain tasks",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "task",
					Value: "",
					Usage: "chain task id",
				},
			},
			Action: func(c *cli.Context) error {
				taskID := c.String("task")
				if err := retry(taskID); err != nil {
					return cli.NewExitError(err.Error(), 1)
				}
				return nil
			},
		},
	}

	// Run the CLI app
	app.Run(os.Args)
}

func loadConfig() (*config.Config, error) {
	if configPath != "" {
		return config.NewFromYaml(configPath, true)
	}

	return config.NewFromEnvironment(true)
}

func startServer() (*machinery.Server, error) {
	cnf, err := loadConfig()
	if err != nil {
		return nil, err
	}

	// Create server instance
	server, err := machinery.NewServer(cnf)
	if err != nil {
		return nil, err
	}

	// Register tasks
	tasks := map[string]interface{}{
		"init":    exampletasks.InitSetup,
		"install": exampletasks.InstallSoft,
		"check":   exampletasks.Check,
	}

	return server, server.RegisterTasks(tasks)
}

func worker() error {
	consumerTag := "machinery_worker"

	server, err := startServer()
	if err != nil {
		return err
	}

	// The second argument is a consumer tag
	// Ideally, each worker should have a unique tag (worker1, worker2 etc)
	worker := server.NewWorker(consumerTag, 0)

	// Here we inject some custom code for error handling,
	// start and end of task hooks, useful for metrics for example.
	errorhandler := func(err error) {
		log.ERROR.Println("I am an error handler:", err)
	}

	pretaskhandler := func(signature *tasks.Signature) {
		log.INFO.Println("I am a start of task handler for:", signature.Name)
	}

	posttaskhandler := func(signature *tasks.Signature) {
		log.INFO.Println("I am an end of task handler for:", signature.Name)
	}

	worker.SetPostTaskHandler(posttaskhandler)
	worker.SetErrorHandler(errorhandler)
	worker.SetPreTaskHandler(pretaskhandler)

	return worker.Launch()
}

func NewInstallTask(ip, soft string) (*tasks.Chain, error) {
	meta := make(map[string]string)
	meta["xxx"] = "step meta"
	signature1 := tasks.NewJob("init", "环境初始化", meta, tasks.NewStringArg(ip))
	signature1.RetryCount = 2
	signature2 := tasks.NewJob("install", "发起装包", meta, tasks.NewStringArg(ip), tasks.NewStringArg(soft))
	signature3 := tasks.NewJob("check", "检查环境", meta, tasks.NewStringArg(ip), tasks.NewStringArg(soft))
	return tasks.NewChain(map[string]string{"task": "1.1.1.1上架任务", "ip": "2.2.2.2"}, signature1, signature2, signature3)
}

func send() error {

	server, err := startServer()
	if err != nil {
		return err
	}

	chain, err := NewInstallTask("1.1.1.1", "blob")
	if err != nil {
		panic(err)
	}

	chain2, err := NewInstallTask("2.2.2.2", "blob")
	if err != nil {
		panic(err)
	}

	chainGroups, err := tasks.NewChainTasks(map[string]string{"task": "批量上架任务"}, chain, chain2)
	if err != nil {
		panic(err)
	}

	taskID, err := server.SendGroupChain(chainGroups)
	if err != nil {
		// failed to send the chain
		// do something with the error
		fmt.Println(err)
	}

	fmt.Println(taskID)

	return nil
}

func queryTask(UUID string) error {
	fmt.Println(UUID)
	server, err := startServer()
	if err != nil {
		return err
	}
	states, err := server.GetChainTaskStatus(UUID)
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Println(states.String())
	return nil
}

func queryGroupTask(UUID string) error {
	fmt.Println(UUID)
	server, err := startServer()
	if err != nil {
		return err
	}
	states, err := server.GetChainTasksStatus(UUID)
	if err != nil {
		return err
	}

	fmt.Println(states.String())
	fmt.Println(states.IsCompleted())
	currentStates := states.CurrentState()
	for k, v := range currentStates {
		fmt.Println(k, v.Signature.Name, v.Signature.StepName, v.State, v.Error, v.EndAt)
	}
	return nil
}

func queryTaskByID(UUID string) error {
	fmt.Println(UUID)
	server, err := startServer()
	if err != nil {
		return err
	}
	data, err := server.QueryTaskByID(UUID)
	if err != nil {
		return err
	}

	utils.PrettyJson(data)
	return nil
}

func queryTasks() error {
	server, err := startServer()
	if err != nil {
		return err
	}
	data, err := server.QueryTasksByTag(time.Now().Add(-60*time.Minute), time.Now(), map[string]string{"task": "批量上架任务"})
	if err != nil {
		return err
	}

	utils.PrettyJson(data)
	return nil
}

func queryChainTask(uuid string) error {
	server, err := startServer()
	if err != nil {
		return err
	}
	data, err := server.QueryChainTaskByID(uuid)
	if err != nil {
		return err
	}

	utils.PrettyJson(data)
	return nil
}

func skip(uuid string) error {
	server, err := startServer()
	if err != nil {
		return err
	}
	return server.SkipAndContinueChainTask(uuid)
}

func retry(uuid string) error {
	server, err := startServer()
	if err != nil {
		return err
	}
	return server.RetryChainTask(uuid)
}

func cancel(uuid string) error {
	server, err := startServer()
	if err != nil {
		return err
	}
	return server.CancelChainTask(uuid)
}
