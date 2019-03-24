package dag

import (
	"fmt"
)

type DagGraph struct {
	Graph   *AcyclicGraph
	NodeMap map[interface{}]interface{}
	RevisitNodeMap map[interface{}]*RevisitInfo
}

type RevisitInfo struct {
	NodeName       string
	RevisitOrdinal int
}

func (info *RevisitInfo) GetNextNodeName() string {
	info.RevisitOrdinal++
	return fmt.Sprintf("%s@%d", info.NodeName, info.RevisitOrdinal)
}
