package server

import (
	"context"
	"net/http"
	"ocf/internal/common"
	"ocf/internal/common/process"
	"ocf/internal/protocol"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
)

func StartServer() {
	protocol.InitializeMyself()
	_, cancelCtx := protocol.GetCRDTStore()
	defer cancelCtx()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	defer stop()

	initTracer()
	// gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.Use(corsHeader())
	r.Use(gin.Recovery())

	go protocol.StartTicker()
	subProcess := viper.GetString("subprocess")
	if subProcess != "" {
		go process.StartCriticalProcess(subProcess)
	}
	v1 := r.Group("/v1")
	{
		v1.GET("/health", healthStatusCheck)
		crdtGroup := v1.Group("/dnt")
		{
			crdtGroup.GET("/table", getDNT)
			crdtGroup.GET("/peers", listPeers)
			crdtGroup.GET("/peers_status", listPeersWithStatus)
			crdtGroup.GET("/bootstraps", listBootstraps)
			crdtGroup.GET("/stats", getResourceStats) // Add resource manager stats endpoint
			crdtGroup.POST("/_node", updateLocal)
			crdtGroup.DELETE("/_node", deleteLocal)
		}
		p2pGroup := v1.Group("/p2p")
		{
			p2pGroup.PATCH("/:peerId/*path", P2PForwardHandler)
			p2pGroup.POST("/:peerId/*path", P2PForwardHandler)
			p2pGroup.GET("/:peerId/*path", P2PForwardHandler)
		}
		globalServiceGroup := v1.Group("/service")
		{
			globalServiceGroup.GET("/:service/*path", GlobalServiceForwardHandler)
			globalServiceGroup.POST("/:service/*path", GlobalServiceForwardHandler)
			globalServiceGroup.PATCH("/:service/*path", GlobalServiceForwardHandler)
		}
		serviceGroup := v1.Group("/_service")
		{
			serviceGroup.GET("/:service/*path", ServiceForwardHandler)
			serviceGroup.POST("/:service/*path", ServiceForwardHandler)
			serviceGroup.PATCH("/:service/*path", ServiceForwardHandler)
		}
	}
	p2plistener := P2PListener()
	srv := &http.Server{
		Addr:    "0.0.0.0:" + viper.GetString("port"),
		Handler: r,
	}
	go func() {
		err := http.Serve(p2plistener, r)
		if err != nil {
			common.Logger.Errorf("http.Serve: %s", err)
		}
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			common.ReportError(err, "Server failed to start")
		}
	}()
	go func() {
		protocol.RegisterLocalServices()
	}()
	<-ctx.Done()
	// shutting down...
	protocol.DeleteNodeTable()
	protocol.ClearCRDTStore()
	time.Sleep(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	common.Logger.Info("Shutting down server gracefully")
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		common.ReportError(err, "Server shutdown failed")
	}
	common.Logger.Info("Server exiting")
}
