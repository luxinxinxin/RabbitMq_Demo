package iface

type IDaemon interface {
	Start() error
	Stop()
}

type IHTTPServer interface {
	IDaemon
}
