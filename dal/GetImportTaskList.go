package dal

import (
	"database/sql"
	"mysql-api-server/base"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// 任务总数
func GetImportTaskList(result *(base.ImportTaskListResult), ListDefine base.LISTDEFINE, Username string) {
	re := base.GetListResult{
		BaseHttpResult: base.BaseHttpResult{
			Code:    1,
			Message: "failed",
		},
	}
	ClusterList := GetClusterList(&re, Username)
	if re.Message == "failed" || re.Code != http.StatusOK {
		re.Message = "get GetCluster List failed from: " + Username
		return
	}
	for _, Cluster := range ClusterList.Clusters {
		Db, err := Login(*Cluster)
		if err != nil {
			log.Println("Username: " + Username + " Login Error in " + *Cluster + " errmsg: " + err.Error())
			continue
		}
		defer Db.Close()
		DatabaseList := GetDatabaseList(&re, Username, *Cluster)
		if re.Message == "failed" || re.Code != http.StatusOK {
			log.Println("get DatabaseList failed! " + "username: " + Username + " Cluster: " + *Cluster)
			continue
		}

		for _, Database := range DatabaseList.Databases {
			GetRoutineLoadList(Db, *Cluster, *Database, result)
			GetHdfsLoadList(Db, *Cluster, *Database, result)
			GetStreamLoadList(Db, *Cluster, *Database, result)
		}
	}
	//sort by Create Time
	sort.Sort(base.ByCreateTIme{result.Data})
	ListDefine.ToTal = base.ByCreateTIme{result.Data}.Len()
	if ListDefine.ToTal == 0 || ListDefine.ToTal/ListDefine.PageSize < (ListDefine.Index-1) {
		result.Data = nil
	} else if ListDefine.ToTal/ListDefine.PageSize < ListDefine.Index {
		result.Data = result.Data[(ListDefine.Index-1)*ListDefine.PageSize:]
	} else {
		result.Data = result.Data[(ListDefine.Index-1)*ListDefine.PageSize : ListDefine.Index*ListDefine.PageSize]
	}

	result.Total = ListDefine.ToTal
	result.Code = http.StatusOK
	result.Message = "Success"
}

//Cluster 提前关联了  这里只需要传入Database
func GetRoutineLoadList(Db *sql.DB, Cluster string, Database string, result *(base.ImportTaskListResult)) {
	result2, err := Db.Exec("use " + Database + ";")
	if err != nil {
		log.Println("Cluster :" + Cluster + "use  Database: " + Database + "err! msg: " + err.Error())
		result.Message = err.Error()
		return
	}
	_, err = result2.LastInsertId()
	if err != nil {
		log.Println("Cluster :" + Cluster + "use  Database: " + Database + "err! msg: " + err.Error())
		result.Message = err.Error()
		return
	}
	log.Println("show all routine load from " + Database)
	rows, err := Db.Query("show all routine load;")
	//Cluster  Db
	if err != nil {
		log.Println("Cluster :" + Cluster + "Database: " + Database)
		log.Println("show all routine load err! msg" + err.Error())
		result.Message = err.Error()
		return
	}
	defer rows.Close()

	for rows.Next() {
		var Loadtask base.Task
		var reasonOfStateChangedTmp sql.NullString
		var errorLogUrlsTmp sql.NullString
		var other interface{}

		if err = rows.Scan(&(Loadtask.Id), &(Loadtask.Label), &(Loadtask.CreateTime), &(Loadtask.PauseTime), &(Loadtask.EndTime),
			&(Loadtask.Database), &(Loadtask.TableName), &(Loadtask.State), &(Loadtask.DataSourceType), &(Loadtask.CurrentTaskNum),
			&(Loadtask.JobProperties), &(Loadtask.DataSourceProperties), &(Loadtask.CustomProperties), &(Loadtask.Statistic), &(Loadtask.Progress),
			&(reasonOfStateChangedTmp), &(errorLogUrlsTmp)); err != nil {
			if err = rows.Scan(&(Loadtask.Id), &(Loadtask.Label), &(Loadtask.CreateTime), &(Loadtask.PauseTime), &(Loadtask.EndTime),
				&(Loadtask.Database), &(Loadtask.TableName), &(Loadtask.State), &(Loadtask.DataSourceType), &(Loadtask.CurrentTaskNum),
				&(Loadtask.JobProperties), &(Loadtask.DataSourceProperties), &(Loadtask.CustomProperties), &(Loadtask.Statistic), &(Loadtask.Progress),
				&(reasonOfStateChangedTmp), &(errorLogUrlsTmp), &other); err != nil {
				result.Message = err.Error()
				return
			}
		}
		if reasonOfStateChangedTmp.Valid {
			Loadtask.ReasonOfStateChanged = reasonOfStateChangedTmp.String
		}
		if errorLogUrlsTmp.Valid {
			Loadtask.ErrorLogUrls = errorLogUrlsTmp.String
		}
		Loadtask.ImportType = "KAFKA"
		Loadtask.Type = "ROUTINE LOAD"
		Loadtask.Cluster = Cluster
		Loadtask.Database = Database
		result.Data = append(result.Data, &Loadtask)
	}
}

func GetHdfsLoadList(Db *sql.DB, Cluster string, Database string, result *(base.ImportTaskListResult)) {
	//show load
	log.Println("show load from " + Database + ";")
	rows, err := Db.Query("show load from " + Database + ";")
	if err != nil {
		result.Message = err.Error()
		return
	}
	defer rows.Close()
	for rows.Next() {
		var Loadtask base.Task
		var errorLogUrlsTmp sql.NullString
		if err = rows.Scan(&(Loadtask.Id), &(Loadtask.Label), &(Loadtask.State), &(Loadtask.Progress), &(Loadtask.Type),
			&(Loadtask.EtlInfo), &(Loadtask.TaskInfo), &(Loadtask.ErrorMsg), &(Loadtask.CreateTime), &(Loadtask.EtlStartTime),
			&(Loadtask.EtlFinishTime), &(Loadtask.LoadStartTime), &(Loadtask.LoadFinishTime), &(errorLogUrlsTmp), &(Loadtask.TransactionStatus)); err != nil {
			result.Message = err.Error()
			return
		}
		if errorLogUrlsTmp.Valid {
			Loadtask.ErrorLogUrls = errorLogUrlsTmp.String
		}
		Loadtask.ImportType = "HDFS"
		Loadtask.Cluster = Cluster
		if _, OK := ClusterConfig[Cluster]; !OK {
			result.Message = "not find cluster:" + Cluster + "int Superset_db!"
			continue
		}
		Loadtask.Database = Database
		result.Data = append(result.Data, &Loadtask)
	}
}

func GetStreamLoadList(Db *sql.DB, Cluster string, Database string, result *(base.ImportTaskListResult)) {
	//show
	rows, err := Db.Query("show proc '/transactions';")
	if err != nil {
		result.Message = err.Error()
		return
	}

	defer rows.Close()
	for rows.Next() {

		var DbId int
		var dbname string
		var bpresence = false

		if err = rows.Scan(&(DbId), &(dbname)); err != nil {
			result.Message = err.Error()
			return
		}
		if dbname == "default_cluster:"+Database {
			//running
			rows, err = Db.Query("show proc '/transactions/" + strconv.Itoa(DbId) + "/running';")
			log.Println("show proc '/transactions/" + strconv.Itoa(DbId) + "/running';")
			if err != nil {
				log.Println("err: " + err.Error())
				result.Message = err.Error()
				return
			}
			for rows.Next() {
				var Loadtask base.Task
				Loadtask.ImportType = "HIVE"
				Loadtask.Cluster = Cluster
				Loadtask.Database = Database
				Loadtask.ImportType = "LOCAL_HIVE"

				if err = rows.Scan(&(Loadtask.Id), &(Loadtask.Label), &(Loadtask.Coordinator), &(Loadtask.State), &(Loadtask.LoadJobSourceType),
					&(Loadtask.CreateTime), &(Loadtask.CommitTime), &(Loadtask.FinishTime), &(Loadtask.ErrorMsg), &(Loadtask.ListenerId), &(Loadtask.TimeoutMs)); err != nil {
					log.Println("row scan err! msg: " + err.Error())
					result.Message = err.Error()
					return
				}
				if strings.Contains(Loadtask.Label, "LOCAL") {
					Loadtask.ImportType = "LOCAL"
					if Loadtask.LoadJobSourceType == "BACKEND_STREAMING" {
						result.Data = append(result.Data, &Loadtask)
					}
				} else if strings.Contains(Loadtask.Label, "HIVE") {
					Loadtask.ImportType = "HIVE"
					if Loadtask.LoadJobSourceType == "BACKEND_STREAMING" {
						result.Data = append(result.Data, &Loadtask)
					}
				}
			}

			//finished
			rows, err = Db.Query("show proc '/transactions/" + strconv.Itoa(DbId) + "/finished';")
			log.Println("show proc '/transactions/" + strconv.Itoa(DbId) + "/finished';")
			if err != nil {
				result.Message = err.Error()
				return
			}
			for rows.Next() {
				var Loadtask base.Task
				Loadtask.ImportType = "HIVE"
				Loadtask.Cluster = Cluster
				Loadtask.Database = Database
				Loadtask.ImportType = "LOCAL_HIVE"
				if err = rows.Scan(&(Loadtask.Id), &(Loadtask.Label), &(Loadtask.Coordinator), &(Loadtask.State), &(Loadtask.LoadJobSourceType),
					&(Loadtask.CreateTime), &(Loadtask.CommitTime), &(Loadtask.FinishTime), &(Loadtask.ErrorMsg), &(Loadtask.ErrorReplicasCount), &(Loadtask.ListenerId), &(Loadtask.TimeoutMs)); err != nil {
					log.Println("row scan err! msg: " + err.Error())
					result.Message = err.Error()
					return
				}
				if strings.Contains(Loadtask.Label, "LOCAL") {
					Loadtask.ImportType = "LOCAL"
					if Loadtask.LoadJobSourceType == "BACKEND_STREAMING" {
						result.Data = append(result.Data, &Loadtask)
					}
				} else if strings.Contains(Loadtask.Label, "HIVE") {
					Loadtask.ImportType = "HIVE"
					if Loadtask.LoadJobSourceType == "BACKEND_STREAMING" {
						result.Data = append(result.Data, &Loadtask)
					}
				}
			}

			//stream load spark sql
			rows, err = HiveLoadDb.Query("select * from hive_load.hive_load_job where Dbase ='" + Database + "'")
			log.Println("select * from hive_load.hive_load_job where Dbase ='" + Database + "'")
			if err != nil {
				result.Message = err.Error()
				return
			}
			for rows.Next() {
				var Loadtask base.Task
				Loadtask.ImportType = "HIVE"
				if err = rows.Scan(&(Loadtask.Label), &(Loadtask.Cluster), &(Loadtask.Database), &(Loadtask.TableName), &(Loadtask.CreateTime)); err != nil {
					result.Message = err.Error()
					return
				}
				Loadtask.State = "In preparation"
				//where label not exist
				for _, value := range result.Data {
					if Loadtask.Label == value.Label && Loadtask.Cluster == value.Cluster && Loadtask.Database == Loadtask.Database {
						bpresence = true
						break
					}
				}
				if !bpresence {
					result.Data = append(result.Data, &Loadtask)
				}
				bpresence = false
			}
		}

	}
}
