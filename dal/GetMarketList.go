package dal

import (
	"encoding/json"
	"io/ioutil"
	"mysql-api-server/base"
	"log"
	"net/http"
)

func GetMarketList(result *base.GetListResult, erp string, martName string, bget bool) *base.MarketList {

	Ugdap := base.UgDapMarketListResult{
		BaseHttpResult: base.BaseHttpResult{
			Code:    1,
			Message: "failed",
		},
	}
	MarketList := base.MarketList{}
	if bget {
		MarketList.MarketListResult.Martname = append(MarketList.MarketListResult.Martname, &Marketdefault)
		result.Date = MarketList.MarketListResult
	}

	log.Println(base.GETMARKETLISTURL + "?erp=" + erp + "&status=1")
	resp, err := http.Get(base.GETMARKETLISTURL + "?erp=" + erp + "&status=1")
	if err != nil {
		result.Message = err.Error()
		return nil
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		result.Message = err.Error()
		return nil
	}

	err = json.Unmarshal(body, &Ugdap)
	if err != nil {
		result.Message = err.Error()
		return nil
	}

	for _, Market := range Ugdap.List {
		if !bget {
			if Market.Martname == martName {
				MarketList.Martname = append(MarketList.Martname, &Market.Martname)
				MarketList.Martcode = Market.Martcode
				MarketList.Clustercode = Market.Clustercode
				break
			}
		} else {
			MarketList.Martname = append(MarketList.Martname, &Market.Martname)
			MarketList.Martcode = Market.Martcode
			MarketList.Clustercode = Market.Clustercode
		}
	}
	result.Code = http.StatusOK
	result.Date = MarketList.MarketListResult
	result.Message = "Success"
	return &MarketList
}
