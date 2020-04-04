package api

import (
	"encoding/json"
	"io"
	"mysql-api-server/base"
	"mysql-api-server/dal"
	"log"
	"net/http"
)

//传入的参数为 martName and martProduction
func GetProductionList(w http.ResponseWriter, r *http.Request) {
	//set default result
	re := base.GetListResult{
		BaseHttpResult: base.BaseHttpResult{
			Code:    1,
			Message: "failed",
		},
	}
	defer func() {
		w.Header().Set("Content-Type", "application/json")
		log.Println("GetProductionList ", re)
		res, _ := json.Marshal(re)
		io.WriteString(w, string(res))
	}()

	//TODO: authentication
	//dal.init(r.Header.Get("username"),r.Header.Get("password"),userkey)
	//TODO: check params

	//1 从Cookie 获取erp  r.Cookie=("erp")

	v := r.URL.Query()
	if v.Get("martName") == "" {
		re.Message = "martName Name  Cannot is null!"
		return
	}

	if result := dal.GetUsernameBysso(r); result.Message != "Success" {
		re.Message = result.Message
		re.Code = result.Code
		return
	}

	//get clusterCode by martName
	//Get martuser and cluster
	me := dal.GetMarketList(&re, *dal.Username, v.Get("martName"), false)

	//Get ProductionAccount
	dal.GetProductionList(&re, *dal.Username, me.Clustercode, me.Martcode, v.Get("martName"), "", true)

}
