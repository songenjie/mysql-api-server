package dal

import (
	"encoding/json"
	"io/ioutil"
	"mysql-api-server/base"
	"log"
	"net/http"
)

func GetQueueList(result *base.GetListResult, erp string, cluster string, martUser string, martName string, production string, productionAccount string, bussinessLine string) *base.QueueListResult {

	Ugdap := base.UgDapQueueListResult{
		BaseHttpResult: base.BaseHttpResult{
			Code:    1,
			Message: "failed",
		},
	}
	QueueListResult := base.QueueListResult{}
	QueueListResult.Martname = martName
	QueueListResult.Martproduction = production
	QueueListResult.Martqueue = append(QueueListResult.Martqueue, &Marketdefault)
	result.Date = QueueListResult
	log.Println(base.GETQUEUELISTLISTURL + "?erp=" + erp + "&cluster=" + cluster + "&martUser=" + martUser + "&productionAccount=" + productionAccount + "&bussinessLine=" + bussinessLine + "&appId=" + base.APPID + "&token=" + base.TOKEN)
	resp, err := http.Get(base.GETQUEUELISTLISTURL + "?erp=" + erp + "&cluster=" + cluster + "&martUser=" + martUser + "&productionAccount=" + productionAccount + "&bussinessLine=" + bussinessLine + "&appId=" + base.APPID + "&token=" + base.TOKEN)
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

	for _, Queue := range Ugdap.List {
		QueueListResult.Martqueue = append(QueueListResult.Martqueue, &Queue.QueueCode)
	}
	result.Code = http.StatusOK
	result.Date = QueueListResult
	result.Message = "Success"
	return &QueueListResult
}

func GetMartBusinessLineDetail(result *base.GetListResult, martCode string, clusterCode string) string {

	Ugdap := base.UgMartBusinessLineDetail{
		BaseHttpResult: base.BaseHttpResult{
			Code:    1,
			Message: "failed",
		},
	}
	log.Println(base.GetMartBusinessLineDetail + "?martCode=" + martCode + "&clusterCode=" + clusterCode + "&appId=" + base.APPID + "&token=" + base.TOKEN)
	resp, err := http.Get(base.GetMartBusinessLineDetail + "?martCode=" + martCode + "&clusterCode=" + clusterCode + "&appId=" + base.APPID + "&token=" + base.TOKEN)
	if err != nil {
		result.Message = err.Error()
		return ""
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		result.Message = err.Error()
		return ""
	}

	err = json.Unmarshal(body, &Ugdap)
	if err != nil {
		result.Message = err.Error()
		return ""
	}

	result.Code = http.StatusOK
	result.Message = "Success"
	return Ugdap.MartCode
}

func GetHadoopUserCertificate(result *base.GetListResult, martCode string, user string) string {

	Ugdap := base.UgQueryUsersDetail{
		BaseHttpResult: base.BaseHttpResult{
			Code:    1,
			Message: "failed",
		},
	}
	log.Println(base.GetQueryUsersURL + "?martCode=" + martCode + "&user=" + user + "&appId=" + base.APPID + "&token=" + base.TOKEN)
	resp, err := http.Get(base.GetQueryUsersURL + "?martCode=" + martCode + "&user=" + user + "&appId=" + base.APPID + "&token=" + base.TOKEN)
	if err != nil {
		result.Message = err.Error()
		return ""
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		result.Message = err.Error()
		return ""
	}

	err = json.Unmarshal(body, &Ugdap)
	if err != nil {
		result.Message = err.Error()
		return ""
	}

	result.Code = http.StatusOK
	result.Message = "Success"
	return Ugdap.QueryUsersDetails[0].Token
}

func GetHiveLoadPar(result *base.GetListResult, erp string, cluster string, martUser string, martName string, production string, productionAccount string, bussinessLine string, queueCode string) *base.QUEUELIST {

	Ugdap := base.UgDapQueueListResult{
		BaseHttpResult: base.BaseHttpResult{
			Code:    1,
			Message: "failed",
		},
	}
	HiveLoad := base.QUEUELIST{}
	log.Println(base.GETQUEUELISTLISTURL + "?erp=" + erp + "&cluster=" + cluster + "&martUser=" + martUser + "&productionAccount=" + productionAccount + "&bussinessLine=" + bussinessLine + "&appId=" + base.APPID + "&token=" + base.TOKEN)
	resp, err := http.Get(base.GETQUEUELISTLISTURL + "?erp=" + erp + "&cluster=" + cluster + "&martUser=" + martUser + "&productionAccount=" + productionAccount + "&bussinessLine=" + bussinessLine + "&appId=" + base.APPID + "&token=" + base.TOKEN)
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

	for _, Queue := range Ugdap.List {
		if Queue.QueueCode == queueCode {
			HiveLoad = *Queue
		}
	}
	result.Code = http.StatusOK
	result.Date = HiveLoad
	result.Message = "Success"
	return &HiveLoad
}
