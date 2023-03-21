package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/yunxiaozhao/Konsensus/network"
	"github.com/yunxiaozhao/Konsensus/pbft"
	"github.com/yunxiaozhao/Konsensus/util"
)

func main() {
	port := os.Args[1]
	util.ReadConfig()
	defer util.WriteConfig()
	SetupCloseHandler()
	if port == util.Config.LeaderPort {
		server := network.Server{}
		server.StartServer()
	} else {
		node := pbft.NewNode("127.0.0.1:" + port)
		node.Start()
	}

}

func SetupCloseHandler() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		util.WriteConfig()
		os.Exit(0)
	}()
}
