package pbft

import (
	"github.com/MonteCarloClub/Krypto/sm2"
	"github.com/MonteCarloClub/log"
	"github.com/gin-gonic/gin"

	"net/http"
)

type node struct {
	port string
}

func NewNode(port string) *node {
	return &node{port: port}
}

func (n *node) Start() {
	r := gin.Default()
	r.POST("/verify", n.verifyHandler)
	r.Run(n.port)
}

func (n *node) verifyHandler(c *gin.Context) {
	log.Info("===============preprepare phase begins==================")
	var msg PrePrepareMsg
	if err := c.BindJSON(&msg); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if msg.CryptoMethod == "sm" {
		pubKey, err := sm2.RawBytesToPublicKey([]byte(msg.PubKey))
		if err != nil {
			log.Error("fail to convert public key", "err", err)
			return
		}
		if sm2.Verify(pubKey, nil, []byte(msg.Data), []byte(msg.SignResult)) {
			log.Info("Preprepare pack received, signature verified, process to prepare")
		} else {
			log.Error("Signature verification failed!!!")
			c.JSON(http.StatusOK, gin.H{"status": "field"})
			return
		}
	}
	log.Info("===============prepare phase begins==================")
	c.JSON(http.StatusOK, gin.H{"status": "ok"})

}

