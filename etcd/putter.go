package etcd

import (
	"context"
	"time"

	"github.com/MonteCarloClub/log"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Putter struct {
	EtcdClientKv clientv3.KV
}

func (p *Putter) InitEtcdClient() {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"106.14.244.78:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Error("fail to init etcd client", "err", err)
		return
	}
	p.EtcdClientKv = clientv3.NewKV(client)
	log.Info("etcd client inited")
}

func (p *Putter) PutToEtcdKv(key string, value string) {
	_, err := p.EtcdClientKv.Put(context.TODO(), key, value)
	if err != nil {
		log.Error("fail to put kv to etcd", "key", key, "value", value, "err", err)
	}
}

func (p *Putter) GetFromEtcdKv(key string) string {
	response, err := p.EtcdClientKv.Get(context.TODO(), key)
	if err != nil {
		log.Error("fail to get kv from etcd", "key", key)
		return ""
	}
	if len(response.Kvs) < 1 || len(response.Kvs) > 1 {
		log.Error("illegal kv from etcd", "key", key)
		return ""
	}
	value := string(response.Kvs[0].Value)
	log.Info("kv from etcd got", "key", key, "value", value)
	return value
}

func (p *Putter) DeleteFromEtcdKv(key string) {
	_, _ = p.EtcdClientKv.Delete(context.TODO(), key)
}
