module github.com/can1357/fdbgo/bench

go 1.27

require (
	github.com/apple/foundationdb/bindings/go v0.0.0-20260924201714-3ce230b69616
	github.com/can1357/fdbgo v0.0.0
)

require (
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

replace github.com/can1357/fdbgo => ../
