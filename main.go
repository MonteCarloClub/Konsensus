package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/yunxiaozhao/Konsensus/network"
	"github.com/yunxiaozhao/Konsensus/util"
)

func main() {
	SetupCloseHandler()
	server := network.Server{}
	server.StartServer()
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
