package crawl

import (
	"context"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const (
	uri      = "bolt://neo4j-db:7687"
	username = "neo4j"
	password = "11111111"
)

type cqlconnection struct {
	uri      string
	username string
	password string
}

func NewCQLConnection(ctx context.Context) *cqlconnection {

	cn := &cqlconnection{
		uri:      uri,
		username: username,
		password: password,
	}
	return cn
}

func (cn *cqlconnection) CreateOrUpdateNode(ctx context.Context, id string, ip string, iffindnode bool, enr string, addtime string) (string, error) {
	driver, err := neo4j.NewDriverWithContext(cn.uri, neo4j.BasicAuth(cn.username, cn.password, ""))
	if err != nil {
		return "", err
	}
	defer driver.Close(ctx)

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	cql := `
    MERGE (n:Node {id: $id})
    ON CREATE SET n.ip = $ip, n.iffindnode = $iffindnode, n.enr = $enr, n.addtime = $addtime
    ON MATCH SET n.ip = $ip, n.iffindnode = CASE WHEN $iffindnode THEN true ELSE n.iffindnode END, n.enr = $enr, n.addtime = $addtime
    RETURN n.id + ', from node ' + id(n)`

	result, err := session.Run(ctx, cql, map[string]interface{}{
		"id":         id,
		"ip":         ip,
		"iffindnode": iffindnode,
		"enr":        enr,
		"addtime":    addtime,
	})
	if err != nil {
		return "", err
	}
	if result.Next(ctx) {
		return result.Record().Values[0].(string), nil
	}
	return "", result.Err()
}

func (cn *cqlconnection) ProcessInteractionNode(ctx context.Context, id string, ip string, enr string, addtime string) (string, error) {
	// 调用 CreateOrUpdateNode 以创建或更新节点，将 iffindnode 设置为 true
	return cn.CreateOrUpdateNode(ctx, id, ip, true, enr, addtime)
}

func (cn *cqlconnection) ProcessNeighborNode(ctx context.Context, interactionNodeId string, neighborId, neighborIp string, neighborEnr string, neighborAddtime string, neighborDistance int) error {
	// 首先尝试创建或更新邻居节点，这里假设 iffindnode 对于邻居节点是 false
	_, err := cn.CreateOrUpdateNode(ctx, neighborId, neighborIp, false, neighborEnr, neighborAddtime)
	if err != nil {
		return err
	}

	// 然后创建从交互节点到这个邻居节点的关系，包括距离
	err = cn.CreateRelation(ctx, interactionNodeId, neighborId, neighborDistance)
	if err != nil {
		return err
	}

	return nil
}

func (cn *cqlconnection) CreateRelation(ctx context.Context, fromId, toId string, distance int) error {
	driver, err := neo4j.NewDriverWithContext(cn.uri, neo4j.BasicAuth(cn.username, cn.password, ""))
	if err != nil {
		return err
	}
	defer driver.Close(ctx)

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	cql := "MATCH (a:Node {id: $fromId}), (b:Node {id: $toId}) MERGE (a)-[r:CONNECTS]->(b) SET r.distance = $distance"

	result, err := session.Run(ctx, cql, map[string]interface{}{
		"fromId":   fromId,
		"toId":     toId,
		"distance": distance,
	})
	if err != nil {
		return err
	}

	// 检查结果中是否存在记录
	if result.Next(ctx) {
		// 实际上如果是创建关系，通常我们不需要返回信息，除非你要返回关系的特定属性
		return nil
	}

	return result.Err()
}

func (cn *cqlconnection) DeleteExistingRelations(ctx context.Context, nodeId string) error {
	driver, err := neo4j.NewDriverWithContext(cn.uri, neo4j.BasicAuth(cn.username, cn.password, ""))
	if err != nil {
		return err
	}
	defer driver.Close(ctx)

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// CQL 语句删除所有出站关系
	cql := "MATCH (n:Node {id: $nodeId})-[r:CONNECTS]->() DELETE r"

	// 使用session.Run执行CQL语句
	_, err = session.Run(ctx, cql, map[string]interface{}{
		"nodeId": nodeId,
	})
	if err != nil {
		return err
	}

	// 由于删除操作不返回结果，我们不需要检查result.Next()
	return nil
}
