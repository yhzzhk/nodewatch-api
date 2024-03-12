// Copyright 2021 ChainSafe Systems
// SPDX-License-Identifier: LGPL-3.0-only

package crawl

import (
	"context"
	"crypto/ecdsa"
	"eth2-crawler/crawler/p2p"
	reqresp "eth2-crawler/crawler/rpc/request"
	"eth2-crawler/crawler/util"
	"eth2-crawler/graph/model"
	"eth2-crawler/models"
	ipResolver "eth2-crawler/resolver"
	"eth2-crawler/store/peerstore"
	"eth2-crawler/store/record"
	"fmt"
	"time"

	"github.com/protolambda/zrnt/eth2/beacon/common"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/enode"
	ma "github.com/multiformats/go-multiaddr"
)

type crawler struct {
	disc            resolver
	peerStore       peerstore.Provider
	historyStore    record.Provider
	ipResolver      ipResolver.Provider
	iter            enode.Iterator
	nodeCh          chan *enode.Node
	privateKey      *ecdsa.PrivateKey
	host            p2p.Host
	jobs            chan *models.Peer
	jobsConcurrency int
	semaphoreChan   chan struct{} // 用于控制邻居节点发现并发的 channel
}

// resolver holds methods of discovery v5
type resolver interface {
	Ping(n *enode.Node) error
	LookupWorker(n *enode.Node) ([]*enode.Node, error)
	LookupWorkerall(n *enode.Node) ([]*enode.Node, error)
}

// newCrawler inits new crawler service
func newCrawler(disc resolver, peerStore peerstore.Provider, historyStore record.Provider,
	ipResolver ipResolver.Provider, privateKey *ecdsa.PrivateKey, iter enode.Iterator,
	host p2p.Host, jobConcurrency int, maxConcurrentTasks int) *crawler {
	c := &crawler{
		disc:            disc,
		peerStore:       peerStore,
		historyStore:    historyStore,
		ipResolver:      ipResolver,
		privateKey:      privateKey,
		iter:            iter,
		nodeCh:          make(chan *enode.Node),
		host:            host,
		jobs:            make(chan *models.Peer, jobConcurrency),
		jobsConcurrency: jobConcurrency,
		semaphoreChan:   make(chan struct{}, maxConcurrentTasks), // 初始化 channel，容量为 maxConcurrentTasks
	}
	return c
}

// start runs the crawler
func (c *crawler) start(ctx context.Context) {
	doneCh := make(chan enode.Iterator)
	go c.runIterator(ctx, doneCh, c.iter)
	for {
		select {
		case n := <-c.nodeCh:
			c.storePeer(ctx, n)
		case <-doneCh:
			// crawling finished
			log.Info("finished iterator")
			return
		}
	}
}

// runIterator uses the node iterator and sends node data through channel
func (c *crawler) runIterator(ctx context.Context, doneCh chan enode.Iterator, it enode.Iterator) {
	defer func() { doneCh <- it }()
	for it.Next() {
		select {
		case c.nodeCh <- it.Node():
			// fmt.Printf("nodech通道情况: len(nodeCh)=%d, capnodeCh)=%d\n", len(c.nodeCh), cap(c.nodeCh))
		case <-ctx.Done():
			return
		}
	}
}

func (c *crawler) storePeer(ctx context.Context, node *enode.Node) {
	// only consider the node having tcp port exported
	if node.TCP() == 0 {
		return
	}
	// filter only eth2 nodes
	eth2Data, err := util.ParseEnrEth2Data(node)
	// fmt.Printf("从enr解析出来的eth2data是%v\n", eth2Data)
	if err != nil { // not eth2 nodes
		log.Error("error in parseenreth2data", log.Ctx{"err": err})
		return
	}
	log.Debug("found a eth2 node", log.Ctx{"node": node})

	// get basic info
	peer, err := models.NewPeer(node, eth2Data)
	if err != nil {
		return
	}

	// save to db if not exists
	err = c.peerStore.Create(ctx, peer)
	if err != nil {
		log.Error("err inserting peer", log.Ctx{"err": err, "peer": peer.String()})
	}

	// //请求eth2节点的邻居节点信息
	// enodes, err := c.disc.LookupWorker(node)
	// if err != nil {
	// 	fmt.Println("findnode失败:", node.ID().GoString())
	// } else {
	// 	fmt.Println("findnode返回邻居节点数量:", len(enodes))
	// 	// err = AddPeerToNeo4j(node, enodes)
	// 	// if err != nil {
	// 	// 	fmt.Printf("节点%v的邻居节点关系写入失败:%v\n", node, err)
	// 	// }
	// 	// 握手之后请求多次节点邻居
	// 	enodesall, err := c.disc.LookupWorkerall(node)
	// 	if err != nil {
	// 		fmt.Println("节点FindAllNeighbors失败:", node.ID().GoString())
	// 	} else {
	// 		fmt.Println("邻居节点数量:", len(enodesall))
	// 		// err = AddPeerToNeo4j(node, enodesall)
	// 		// if err != nil {
	// 		// 	fmt.Printf("节点%v的邻居节点关系写入失败:%v\n", node, err)
	// 		// }
	// 		enodes = append(enodes, enodesall...)
	// 		// 去重
	// 		uniqueNodes := make([]*enode.Node, 0)
	// 		seen := make(map[enode.ID]struct{})
	// 		for _, node := range enodes {
	// 			if _, ok := seen[node.ID()]; !ok {
	// 				seen[node.ID()] = struct{}{}
	// 				uniqueNodes = append(uniqueNodes, node)
	// 			}
	// 		}
	// 		err = AddPeerToNeo4j(node, uniqueNodes)
	// 		if err != nil {
	// 			fmt.Printf("节点%v的邻居节点关系写入失败:%v\n", node, err)
	// 		}
	// 	}
	// }

	// 异步执行拓扑探测
	go func(n *enode.Node) {
		// 在进行拓扑探测之前，尝试向 semaphoreChan 发送数据
		// 如果 channel 已满，这里会阻塞，直到 channel 有空间
		c.semaphoreChan <- struct{}{}

		// 确保在函数退出时，从 semaphoreChan 中接收数据，释放一个位置
		defer func() {
			<-c.semaphoreChan
		}()

		// 执行拓扑探测逻辑
		enodes, err := c.disc.LookupWorker(n)
		if err != nil {
			fmt.Println("findnode失败:", n.ID().GoString())
			return
		}
		enodesall, err := c.disc.LookupWorkerall(n)
		if err != nil {
			fmt.Println("LookupWorkerall失败:", n.ID().GoString())
			return
		}
		fmt.Println("邻居节点数量:", len(enodesall))
		enodes = append(enodes, enodesall...)
		// 去重
		uniqueNodes := make([]*enode.Node, 0)
		seen := make(map[enode.ID]struct{})
		for _, node := range enodes {
			if _, ok := seen[node.ID()]; !ok {
				seen[node.ID()] = struct{}{}
				uniqueNodes = append(uniqueNodes, node)
			}
		}
		err = AddPeerToNeo4j(node, uniqueNodes)
		if err != nil {
			fmt.Printf("节点%v的邻居节点关系写入失败:%v\n", node, err)
		}
	}(node)
}

func AddPeerToNeo4j(n *enode.Node, r []*enode.Node) error {
	// 创建 cqlconnection 实例
	ctx := context.Background()
	conn := NewCQLConnection(ctx)

	// 处理交互节点 n
	interactionNodeId := n.ID().String()
	interactionNodeIp := n.IP().String()
	interactionNodeEnr := n.String() // 获取ENR的方式

	addtime := time.Now().Format(time.RFC3339) // 记录节点添加和更新时间

	// 更新或创建交互节点，并将 iffindnode 设置为 true
	_, err := conn.CreateOrUpdateNode(ctx, interactionNodeId, interactionNodeIp, true, interactionNodeEnr, addtime)
	if err != nil {
		// 如果发生错误，记录并处理
		fmt.Printf("Failed to create or update interaction node: %v\n", err)
		return err
	}
	// 如果交互节点已存在，删除其所有现有的向外关系
	err = conn.DeleteExistingRelations(ctx, interactionNodeId)
	if err != nil {
		// 如果发生错误，记录并处理
		fmt.Printf("Failed to delete existing relations for interaction node: %v\n", err)
		return err
	}

	// 处理邻居节点列表 r
	for _, neighbor := range r {
		neighborId := neighbor.ID().String()
		neighborIp := neighbor.IP().String()
		neighborEnr := neighbor.String()
		// 这里需要一种方法来计算或获取两个节点之间的距离
		neighborDistance := enode.LogDist(n.ID(), neighbor.ID()) - 239

		// 更新或创建邻居节点，并将 iffindnode 设置为 false
		_, err = conn.CreateOrUpdateNode(ctx, neighborId, neighborIp, false, neighborEnr, addtime)
		if err != nil {
			// 如果发生错误，记录并继续处理下一个邻居节点
			fmt.Printf("Failed to create or update neighbor node: %v\n", err)
			continue
		}

		// 创建从交互节点到邻居节点的关系
		err = conn.CreateRelation(ctx, interactionNodeId, neighborId, neighborDistance)
		if err != nil {
			// 如果发生错误，记录并继续处理下一个邻居节点
			fmt.Printf("Failed to create relation: %v\n", err)
			continue
		}
	}

	return nil
}

func (c *crawler) updatePeer(ctx context.Context) {
	c.runBGWorkersPool(ctx)
	for {
		select {
		case <-ctx.Done():
			log.Error("update peer job context was canceled", log.Ctx{"err": ctx.Err()})
		default:
			c.selectPendingAndExecute(ctx)
		}
		time.Sleep(5 * time.Second)
	}
}

func (c *crawler) selectPendingAndExecute(ctx context.Context) {
	// get peers that was updated 24 hours ago
	reqs, err := c.peerStore.ListForJob(ctx, time.Hour*24, c.jobsConcurrency)
	if err != nil {
		log.Error("error getting list from peerstore", log.Ctx{"err": err})
		return
	}
	for _, req := range reqs {
		// update the pr, so it won't be picked again in 24 hours
		// We have to update the LastUpdated field here and cannot rety on the worker to update it
		// That is because the same request will be picked again when it is in worker.
		req.LastUpdated = time.Now().Unix()
		err = c.peerStore.Update(ctx, req)
		if err != nil {
			log.Error("error updating request", log.Ctx{"err": err})
			continue
		}
		select {
		case <-ctx.Done():
			log.Error("update selector stopped", log.Ctx{"err": ctx.Err()})
			return
		default:
			c.jobs <- req
			fmt.Printf("job通道情况: len(jobs)=%d, cap(jobs)=%d\n", len(c.jobs), cap(c.jobs))
		}
	}
}

func (c *crawler) runBGWorkersPool(ctx context.Context) {
	for i := 0; i < c.jobsConcurrency; i++ {
		go c.bgWorker(ctx)
	}
}

func (c *crawler) bgWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Error("context canceled", log.Ctx{"err": ctx.Err()})
			return
		case req := <-c.jobs:
			c.updatePeerInfo(ctx, req)
		}
	}
}

func (c *crawler) updatePeerInfo(ctx context.Context, peer *models.Peer) {
	// update connection status, agent version, sync status
	isConnectable := c.collectNodeInfoRetryer(ctx, peer)
	if isConnectable {
		peer.SetConnectionStatus(true)
		peer.Score = models.ScoreGood
		peer.LastConnected = time.Now().Unix()
		// update geolocation
		if peer.GeoLocation == nil {
			c.updateGeolocation(ctx, peer)
		}
	} else {
		peer.Score--
	}
	// remove the node if it has bad score
	if peer.Score <= models.ScoreBad {
		log.Info("deleting node for bad score", log.Ctx{"peer_id": peer.ID})
		err := c.peerStore.Delete(ctx, peer)
		if err != nil {
			log.Error("failed on deleting from peerstore", log.Ctx{"err": err})
		}
		return
	}
	err := c.peerStore.Update(ctx, peer)
	if err != nil {
		log.Error("failed on updating peerstore", log.Ctx{"err": err})
	}
}

func (c *crawler) collectNodeInfoRetryer(ctx context.Context, peer *models.Peer) bool {
	count := 0
	var err error
	var ag, pv string
	for count < 20 {
		time.Sleep(time.Second * 5)
		count++

		err = c.host.Connect(ctx, *peer.GetPeerInfo())
		if err != nil {
			continue
		}
		// get status
		var status *common.Status
		status, err = c.host.FetchStatus(c.host.NewStream, ctx, peer, new(reqresp.SnappyCompression))
		if err != nil || status == nil {
			continue
		}
		ag, err = c.host.GetAgentVersion(peer.ID)
		if err != nil {
			continue
		} else {
			peer.SetUserAgent(ag)
		}

		pv, err = c.host.GetProtocolVersion(peer.ID)
		if err != nil {
			continue
		} else {
			peer.SetProtocolVersion(pv)
		}

		var protocols []string
		protocols, err = c.host.GetProtocols(peer.ID)
		if err != nil {
			continue
		} else {
			peer.SetProtocols(protocols)
		}

		var alladdrs []ma.Multiaddr
		alladdrs, err = c.host.GetAllAddrs(peer.ID)
		if err != nil {
			continue
		} else {
			fmt.Println("AllAddrs:", alladdrs)
		}

		// set sync status
		peer.SetSyncStatus(int64(status.HeadSlot))
		log.Info("successfully collected all info", peer.Log())
		return true
	}
	// unsuccessful
	log.Error("failed on retryer", log.Ctx{
		"attempt": count,
		"error":   err,
	})
	return false
}

func (c *crawler) updateGeolocation(ctx context.Context, peer *models.Peer) {
	geoLoc, err := c.ipResolver.GetGeoLocation(ctx, peer.IP)
	if err != nil {
		log.Error("unable to get geo information", log.Ctx{
			"error":   err,
			"ip_addr": peer.IP,
		})
		return
	}
	peer.SetGeoLocation(geoLoc)
}

func (c *crawler) insertToHistory() {
	ctx := context.Background()
	// get count
	aggregateData, err := c.peerStore.AggregateBySyncStatus(ctx, &model.PeerFilter{})
	if err != nil {
		log.Error("error getting sync status", log.Ctx{"err": err})
	}

	history := models.NewHistory(aggregateData.Synced, aggregateData.Total)
	err = c.historyStore.Create(ctx, history)
	if err != nil {
		log.Error("error inserting sync status", log.Ctx{"err": err})
	}
}
