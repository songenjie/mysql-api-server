package dal

import (
	"database/sql"
	"encoding/json"
	"mysql-api-server/base"
	"log"
	"net/http"
	"strings"
)

type JobProperties struct {
	Partitions               string `json:"partitions"`               //* 判定无用 DataSourceProperties 有
	ColumnToColumnExpr       string `json:"columnToColumnExpr"`       //COLUMNS
	MaxBatchIntervalS        string `json:"maxBatchIntervalS"`        //max_batch_interval
	WhereExpr                string `json:"whereExpr"`                //WHERE
	MaxBatchSizeBytes        string `json:"maxBatchSizeBytes"`        //max_batch_size
	ColumnSeparator          string `json:"columnSeparator"`          //COLUMNS TERMINATED BY
	MaxErrorNum              string `json:"maxErrorNum"`              //max_error_number
	CurrentTaskConcurrentNum string `json:"currentTaskConcurrentNum"` //desired_concurrent_number
	MaxBatchRows             string `json:"maxBatchRows"`             //max_batch_rows
}

type DataSourceProperties struct {
	Topic                  string `json:"topic"`                  //kafka_topic
	CurrentKafkaPartitions string `json:"currentKafkaPartitions"` //kafka_partitions
	BrokerList             string `json:"brokerList"`             //kafka_broker_list
	Offset                 string `json:"offset"`                 //kafka_offsets
}

// 任务总数
func ExportTask(result *base.RoutineLoadResult) {
	for _, partitionCluster := range PartitionConfig.PartitionClusters {
		Db, err := Login(partitionCluster.PartitionClustername)
		if err != nil {
			result.Message = err.Error()
			return
		}
		defer Db.Close()

		for _, partitionDatbase := range partitionCluster.PartitionDatabases {
			reultUseDatabase, err := Db.Exec("use " + partitionDatbase.PartitionDatabasename + ";")
			if err != nil {
				result.Message = err.Error()
				return
			}
			_, err = reultUseDatabase.LastInsertId()
			if err != nil {
				result.Message = err.Error()
				return
			}
			log.Println("show routine load from " + partitionDatbase.PartitionDatabasename)
			//rows, err := Db.Query("show all routine load;")
			rows, err := Db.Query("show routine load;")
			//Cluster  Db
			if err != nil {
				result.Message = err.Error()
			}
			defer rows.Close()

			for rows.Next() {
				var Loadtask base.Task
				var reasonOfStateChangedTmp sql.NullString
				var errorLogUrlsTmp sql.NullString

				var Kafka KAFKA
				var JobProperties JobProperties
				var DataSourceProperties DataSourceProperties
				var Progress map[string]string
				var CustomProperties map[string]string

				if err = rows.Scan(&(Loadtask.Id), &(Loadtask.Label), &(Loadtask.CreateTime), &(Loadtask.PauseTime), &(Loadtask.EndTime),
					&(Loadtask.Database), &(Loadtask.TableName), &(Loadtask.State), &(Loadtask.DataSourceType), &(Loadtask.CurrentTaskNum),
					&(Loadtask.JobProperties), &(Loadtask.DataSourceProperties), &(Loadtask.CustomProperties), &(Loadtask.Statistic), &(Loadtask.Progress),
					&(reasonOfStateChangedTmp), &(errorLogUrlsTmp)); err != nil {
					result.Message = err.Error()
					return
				}
				if reasonOfStateChangedTmp.Valid {
					Loadtask.ReasonOfStateChanged = reasonOfStateChangedTmp.String
				}
				if errorLogUrlsTmp.Valid {
					Loadtask.ErrorLogUrls = errorLogUrlsTmp.String
				}

				if err := json.Unmarshal([]byte(Loadtask.JobProperties), &JobProperties); err != nil {
					result.Message = err.Error()
					return
				}
				if err := json.Unmarshal([]byte(Loadtask.DataSourceProperties), &DataSourceProperties); err != nil {
					result.Message = err.Error()
					return
				}
				if err := json.Unmarshal([]byte(Loadtask.Progress), &Progress); err != nil {
					result.Message = err.Error()
					return
				}
				if err := json.Unmarshal([]byte(Loadtask.CustomProperties), &CustomProperties); err != nil {
					result.Message = err.Error()
					return
				}

				Kafka.Loadlabel.Cluster = partitionCluster.PartitionClustername
				Kafka.Loadlabel.Database = partitionDatbase.PartitionDatabasename
				Kafka.Loadlabel.LabelName = Loadtask.Label
				Kafka.Loadlabel.Table = Loadtask.TableName

				Kafka.LoadProperties.Columns = JobProperties.ColumnToColumnExpr
				Kafka.LoadProperties.ColumnSeparator = JobProperties.ColumnSeparator[1 : len(JobProperties.ColumnSeparator)-1]
				Kafka.LoadProperties.Partitions = JobProperties.Partitions
				Kafka.LoadProperties.Where = JobProperties.WhereExpr

				//Kafka.DataSourceProperties.KafkaOffsets = DataSourceProperties.Offset
				var Offset strings.Builder
				for _, partitionoffset := range Progress {
					if len(Offset.String()) != 0 {
						Offset.WriteString(",")
					}
					Offset.WriteString(partitionoffset)
				}
				Kafka.DataSourceProperties.KafkaOffsets = Offset.String()

				Kafka.DataSourceProperties.KafkaPartitions = DataSourceProperties.CurrentKafkaPartitions
				Kafka.DataSourceProperties.KafkaTopic = DataSourceProperties.Topic
				Kafka.DataSourceProperties.KafkaBrokerlist = DataSourceProperties.BrokerList

				Kafka.DataSourceProperties.GroupId = CustomProperties["group.id"]
				Kafka.DataSourceProperties.DataType = CustomProperties["data_type"]
				Kafka.DataSourceProperties.UserName = CustomProperties["sasl.username"]
				Kafka.DataSourceProperties.PassWord = CustomProperties["sasl.password"]
				Kafka.DataSourceProperties.ClientId = CustomProperties["client.id"]
				Kafka.DataSourceProperties.Datasource = CustomProperties["datasource"]

				Kafka.JobProperties.DesiredConCurrentNumber = JobProperties.CurrentTaskConcurrentNum
				Kafka.JobProperties.MaxBatchInterval = JobProperties.MaxBatchIntervalS
				Kafka.JobProperties.MaxErrorNumber = JobProperties.MaxErrorNum
				Kafka.JobProperties.MaxBatchRows = JobProperties.MaxBatchRows
				Kafka.JobProperties.MaxBatchSize = JobProperties.MaxBatchSizeBytes

				loadstring, err := Kafka.GetLoadString()
				if err != nil {
					result.Message = err.Error()
					return
				}
				result.Routineload += loadstring + "\n"
			}
		}
	}

	result.Code = http.StatusOK
	result.Message = "Success"
}
