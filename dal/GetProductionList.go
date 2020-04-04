package dal

import (
	"encoding/json"
	"io/ioutil"
	"mysql-api-server/base"
	"log"
	"net/http"
)

func GetProductionList(result *base.GetListResult, erp string, cluster string, martUser string, martName string, martProduction string, bget bool) *base.Productions {
	Ugdap := base.UgDapProductionListResult{
		BaseHttpResult: base.BaseHttpResult{
			Code:    1,
			Message: "failed",
		},
	}
	Productions := base.Productions{}
	if bget {
		Productions.Martproduction = append(Productions.Martproduction, &Marketdefault)
		result.Date = Productions.ProductionListResult
	}
	Productions.Martname = martName
	log.Println(base.GETPRODUCTIONLISTURL + "?erp=" + erp + "&cluster=" + cluster + "&martUser=" + martUser)
	resp, err := http.Get(base.GETPRODUCTIONLISTURL + "?erp=" + erp + "&cluster=" + cluster + "&martUser=" + martUser)
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

	for _, Production := range Ugdap.List {
		if Production.IsPersonal == "false" {
			if !bget && Production.ProductionAccountAlias == martProduction {
				Productions.ProductionAccountCode = Production.ProductionAccountCode //name
				Productions.BuinessLine = Production.BuinessLine                     //mart_bag mart_mobile
				Productions.Martname = Production.ClusterCode                        //10k hope
				break
			}
			Productions.Martproduction = append(Productions.Martproduction, &Production.ProductionAccountAlias)
		}
	}
	result.Code = http.StatusOK
	result.Date = Productions.ProductionListResult
	result.Message = "Success"
	return &Productions
}
