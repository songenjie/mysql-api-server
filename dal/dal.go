package dal

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"gopkg.in/alecthomas/kingpin.v2"
	"io"
	"io/ioutil"
	"mysql-api-server/base"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	SuperSetDb          *sql.DB
	HiveLoadDb          *sql.DB
	HiveLoadConfig      base.MySql
	SupersetConfig      base.MySql
	PartitionConfig     base.PartitionConfig
	HdfsConfig          = make([]base.Hdfs, 20)
	ClusterConfig       = make(map[string]base.MySql)
	AdminUserConfig     = make([]*string, 20)
	Marketdefault       = "不选择"
	app                 = kingpin.New(filepath.Base(os.Args[0]), "The msyql-api-server")
	Username            = app.Flag("username", "if erp not package in api").Default("").String()
	Brokername          = app.Flag("brokername", "set broker load of broker name when hdfs load").Default("hdfs").String()
	ListPort            = app.Flag("listPort", "msyql-api-server list port").Default("6150").String()
	PersistenceDbsTTL   = app.Flag("persistence.dbs-ttl", "Them Minimum interval update the supertset dbs").Default("10m").Duration()
	PartitionDbsTime    = app.Flag("persistence.partition.dbs-time", "The PartitionDbsTime is the time Task to update superset dbs ").Default("3").Int()
	SupersetConfigFile  = app.Flag("supersetconfigfile", "superset config").Default("./conf/superset.json").String()
	HiveLoadConfigFile  = app.Flag("hiveloadconfigfile", "hive load config").Default("./conf/hive_load.json").String()
	HdfsConfigFile      = app.Flag("hdfsconfigfile", "load hdfs config").Default("./conf/hdfs.json").String()
	PartitionConfigFile = app.Flag("partitionconfigfile", "load partition config").Default("./conf/partition.json").String()
	bPartition          = app.Flag("bpartition", "if parition ").Bool()
	AdminuserConfigFile = app.Flag("adminuserconfigfile", "admin user").Default("./conf/admin_user.json").String()
)

func init() {
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))
	if err := Reload(); err != nil {
		panic(err)
	}
	body, err := ioutil.ReadFile(*PartitionConfigFile)
	if err != nil {
		panic(err)
	}
	if err = json.Unmarshal(body, &PartitionConfig); err != nil {
		panic(err)
	}
	if *bPartition {
		if err := Partition(); err != nil {
			panic(err)
		}
	}
}

type DATA interface {
	Unmarshal([]byte) error
	Type() string
	Import(*http.Request, *(base.SubmitImportTask)) error
}

//Judge import type
func GetImportObject(Type string) (DATA, error) {
	switch Type {
	case "HDFS":
		return &HDFS{}, nil
	case "KAFKA":
		return &KAFKA{}, nil
	case "LOCAL":
		return &LOCAL{}, nil
	case "HIVE":
		return &HIVE{}, nil
	default:
		return nil, errors.New("No this Import Type!\n")
	}
}

func GetOffsetDay(Offset int, bReplace bool) string {
	offsetTime, _ := time.ParseDuration(strconv.Itoa(24*Offset) + "h")
	offsetDay := time.Now().Add(offsetTime).Format("2006-01-02")
	if bReplace {
		offsetDay = strings.Replace(offsetDay, "-", "", -1)
	}
	return offsetDay
}

func IfHistoryDay(day string) bool {
	dayint, _ := strconv.Atoi(day[len(day)-4:])
	for _, historyDay := range PartitionConfig.HistoryDays {
		if historyDay.StartDay <= dayint && dayint <= historyDay.EndDay {
			return true
		}
	}
	return false
}

func IfHistoryDayOfBeforeDay(day string) (string, bool) {
	dayint, _ := strconv.Atoi(day[len(day)-4:])
	if dayint == 531 {
		return day[len(day)-8:len(day)-4] + "-06-01", true
	}
	if dayint == 1031 {
		return day[len(day)-8:len(day)-4] + "-11-01", true
	}
	return "", false
}

func Partition() error {
	for _, partitionCluster := range PartitionConfig.PartitionClusters {
		Db, err := Login(partitionCluster.PartitionClustername)
		if err != nil {
			return err
		}
		defer Db.Close()

		for _, partitionDatbase := range partitionCluster.PartitionDatabases {
			for _, partitionTable := range partitionDatbase.PartitionTables {
				startOffsetDay := GetOffsetDay(-partitionTable.PartitionStartOffsetDay+1, true)
				endOffsetDay := GetOffsetDay(partitionTable.PartitionEndOffsetDay, true)
				log.Println(partitionTable.PartitionTablename + ":\n                    " + "startOffsetDay: " + startOffsetDay + "\tendOffsetDay: " + endOffsetDay)

				rows, err := Db.Query("show partitions from " + partitionDatbase.PartitionDatabasename + "." + partitionTable.PartitionTablename)
				if err != nil {
					return err
				}
				defer rows.Close()
				var ifNoNeedtoCreatePartition bool
				var ifNeedtoDeleteAndCreatePartition bool
				for rows.Next() {
					var partitionName string
					var i interface{}
					if err := rows.Scan(&i, &partitionName, &i, &i, &i,
						&i, &i, &i, &i, &i,
						&i, &i, &i, &i); err != nil {
						return err
					}
					//小于 2019111 之前的数据再也不处理了
					if partitionName > partitionTable.PartitionHeadNameBy+"20191101" && !IfHistoryDay(partitionName) && (partitionName < partitionTable.PartitionHeadNameBy+startOffsetDay ||
						partitionName > partitionTable.PartitionHeadNameBy+endOffsetDay) {

						if partitionName == partitionTable.PartitionHeadNameBy+GetOffsetDay(-partitionTable.PartitionStartOffsetDay, true) {
							ifNeedtoDeleteAndCreatePartition = true
						}
						//delete partition
						//ALTER TABLE database.table DROP PARTITION
						sqlOfDropPartition := "ALTER TABLE " + partitionDatbase.PartitionDatabasename + "." + partitionTable.PartitionTablename + " DROP PARTITION " + partitionName
						log.Println(sqlOfDropPartition)
						result, err := Db.Exec(sqlOfDropPartition)
						if err != nil {
							return err
						}

						_, err = result.LastInsertId()
						if err != nil {
							return err
						}

						CreateParttion, bIfCreateHistoryPartion := IfHistoryDayOfBeforeDay(partitionName)
						if bIfCreateHistoryPartion {
							addPartition := "ALTER TABLE " + partitionDatbase.PartitionDatabasename + "." + partitionTable.PartitionTablename +
								" ADD PARTITION " + partitionName + " VALUES LESS THAN (\"" + CreateParttion + " 00:00:00\")"
							log.Println(addPartition)
							result, err := Db.Exec(addPartition)
							if err != nil && !(strings.Contains(err.Error(), "Duplicate")) {
								log.Println(err)
								addPartition := "ALTER TABLE " + partitionDatbase.PartitionDatabasename + "." + partitionTable.PartitionTablename +
									" ADD PARTITION " + partitionName + " VALUES LESS THAN (\"" + CreateParttion + "\")"
								log.Println(addPartition)
								result, err = Db.Exec(addPartition)
								if err != nil {
									return err
								}
							}
							if err == nil {
								_, err = result.LastInsertId()
								if err != nil {
									return err
								}
							}
						}

					} else {
						//append to slice
						partitionTable.Ppartitions = append(partitionTable.Ppartitions, partitionName)
					}
				}

				// 查看是否需要新建partitions
				if ifNeedtoDeleteAndCreatePartition {
					//fix old date no partitions map
					addPartition := "ALTER TABLE " + partitionDatbase.PartitionDatabasename + "." + partitionTable.PartitionTablename +
						" ADD PARTITION " + partitionTable.PartitionHeadNameBy + GetOffsetDay(-partitionTable.PartitionStartOffsetDay, true) +
						" VALUES LESS THAN (\"" + GetOffsetDay(-partitionTable.PartitionStartOffsetDay+1, false) + " 00:00:00\")"
					log.Println(addPartition)
					result, err := Db.Exec(addPartition)
					if err != nil && !(strings.Contains(err.Error(), "Duplicate")) {
						log.Println(err)
						addPartition = "ALTER TABLE " + partitionDatbase.PartitionDatabasename + "." + partitionTable.PartitionTablename +
							" ADD PARTITION " + partitionTable.PartitionHeadNameBy + GetOffsetDay(-partitionTable.PartitionStartOffsetDay, true) +
							" VALUES LESS THAN (\"" + GetOffsetDay(-partitionTable.PartitionStartOffsetDay+1, false) + "\")"
						log.Println(addPartition)
						result, err = Db.Exec(addPartition)
						if err != nil {
							return err
						}
					}
					if err == nil {
						_, err = result.LastInsertId()
						if err != nil {
							return err
						}
					}
				}

				//查看想要的partition 是否存在
				//想要天是否存在  //旧的 暂时不管 因认为管理 这里暂时指判断是否需要加 大于今天的
				//for i := -Ptable.Pstart_offset_day + 1; i <= Ptable.Pend_offset_day; i++ {
				for i := 0; i <= partitionTable.PartitionEndOffsetDay; i++ {
					//get current day

					timeAddToCurrent := GetOffsetDay(i, true)

					for _, partitionName := range partitionTable.Ppartitions {
						if partitionName == partitionTable.PartitionHeadNameBy+timeAddToCurrent {
							//log.Println("true")
							ifNoNeedtoCreatePartition = true
							break
						}
					}
					//不存在这里需要创建
					if !ifNoNeedtoCreatePartition && len(partitionTable.Ppartitions) > 0 {
						//add partition
						//ALTER TABLE  database.table add PARTITION ptime VALUES LESS THAN
						CurrentDayAddOne := GetOffsetDay(i+1, false)
						addPartition := "ALTER TABLE " + partitionDatbase.PartitionDatabasename + "." + partitionTable.PartitionTablename +
							" ADD PARTITION " + partitionTable.PartitionHeadNameBy + timeAddToCurrent + " VALUES LESS THAN (\"" + CurrentDayAddOne + " 00:00:00\")"
						log.Println(addPartition)
						result, err := Db.Exec(addPartition)
						if err != nil && !(strings.Contains(err.Error(), "Invalid date string")) {
							log.Println(err)
							addPartition := "ALTER TABLE " + partitionDatbase.PartitionDatabasename + "." + partitionTable.PartitionTablename +
								" ADD PARTITION " + partitionTable.PartitionHeadNameBy + timeAddToCurrent + " VALUES LESS THAN (\"" + CurrentDayAddOne + "\")"
							log.Println(addPartition)
							result, err = Db.Exec(addPartition)
							if err != nil {
								return err
							}
						}
						if err == nil {
							_, err = result.LastInsertId()
							if err != nil {
								return err
							}
						}
					}
					ifNoNeedtoCreatePartition = false
				}

			}
		}
	}
	return nil
}

func Reload() error {
	//hive Load
	body, err := ioutil.ReadFile(*AdminuserConfigFile)
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, &AdminUserConfig)
	if err != nil {
		return err
	}
	//hive Load
	body, err = ioutil.ReadFile(*HiveLoadConfigFile)
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, &HiveLoadConfig)
	if err != nil {
		return err
	}
	cf := mysql.Config{
		User:                 HiveLoadConfig.User,
		Passwd:               HiveLoadConfig.Pwd,
		Net:                  "tcp",
		Addr:                 HiveLoadConfig.Host + ":" + HiveLoadConfig.Port,
		DBName:               HiveLoadConfig.Name,
		AllowNativePasswords: true,
	}
	connStr := cf.FormatDSN()
	log.Println(connStr)
	if HiveLoadDb, err = sql.Open("mysql", connStr); err != nil {
		return err
	}
	if err = HiveLoadDb.Ping(); err != nil {
		return err
	}
	//superset
	body, err = ioutil.ReadFile(*SupersetConfigFile)
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, &SupersetConfig)
	if err != nil {
		return err
	}
	cf = mysql.Config{
		User:                 SupersetConfig.User,
		Passwd:               SupersetConfig.Pwd,
		Net:                  "tcp",
		Addr:                 SupersetConfig.Host + ":" + SupersetConfig.Port,
		DBName:               SupersetConfig.Name,
		AllowNativePasswords: true,
	}
	connStr = cf.FormatDSN()
	log.Println(connStr)
	if SuperSetDb, err = sql.Open("mysql", connStr); err != nil {
		return err
	}
	if err = SuperSetDb.Ping(); err != nil {
		return err
	}
	//定时任务
	rows, err := SuperSetDb.Query("select database_name,sqlalchemy_uri from dbs where database_name !='main' and  password IS  NULL;;")
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var database_name string
		var sqlalchemy_uri string
		if err := rows.Scan(&database_name, &sqlalchemy_uri); err != nil {
			return err
		}
		var currentdb base.MySql
		currentdb.Name = strings.Split(sqlalchemy_uri, "/")[len(strings.Split(sqlalchemy_uri, "/"))-1]
		r, _ := regexp.Compile("[0-9]+.[0-9]+.[0-9]+.[0-9]+")
		currentdb.Host = r.FindString(sqlalchemy_uri)
		r, _ = regexp.Compile(":[0-9]+")
		currentdb.Port = strings.TrimLeft(r.FindString(sqlalchemy_uri), ":")
		//currentdb.User = strings.TrimRight(sqlalchemy_uri[strings.Index(sqlalchemy_uri, "//")+2:strings.Index(sqlalchemy_uri, "@")], ":XXXXXXXXXX")
		//pwd encode
		//currentdb.Pwd = ""
		currentdb.User = "root"
		currentdb.Pwd = ""
		ClusterConfig[database_name] = currentdb
	}

	rows, err = SuperSetDb.Query("select database_name,sqlalchemy_uri,password from dbs where database_name !='main' and  password IS NOT  NULL;;")
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		var database_name string
		var sqlalchemy_uri string
		var password string
		if err := rows.Scan(&database_name, &sqlalchemy_uri, &password); err != nil {
			return err
		}
		var currentdb base.MySql
		currentdb.Name = strings.Split(sqlalchemy_uri, "/")[len(strings.Split(sqlalchemy_uri, "/"))-1]
		r, _ := regexp.Compile("[0-9]+.[0-9]+.[0-9]+.[0-9]+")
		currentdb.Host = r.FindString(sqlalchemy_uri)
		r, _ = regexp.Compile(":[0-9]+")
		currentdb.Port = strings.TrimLeft(r.FindString(sqlalchemy_uri), ":")
		currentdb.User = "root"
		currentdb.Pwd = ""
		//currentdb.User = strings.TrimRight(sqlalchemy_uri[strings.Index(sqlalchemy_uri, "//")+2:strings.Index(sqlalchemy_uri, "@")], ":XXXXXXXXXX")
		//pwd encode
		//currentdb.Pwd = *Username
		ClusterConfig[database_name] = currentdb
	}
	for k, v := range ClusterConfig {
		log.Println(k, v)
	}
	body, err = ioutil.ReadFile(*HdfsConfigFile)
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, &HdfsConfig)
	if err != nil {
		return err
	}
	log.Println(HdfsConfig)
	return nil
}

func Login(Cluster string) (*sql.DB, error) {
	if _, OK := ClusterConfig[Cluster]; !OK {
		return nil, errors.New("not find cluster:" + Cluster + "int Superset_db!")
	}
	return sql.Open("mysql", ClusterConfig[Cluster].User+":"+ClusterConfig[Cluster].Pwd+"@tcp("+ClusterConfig[Cluster].Host+
		":"+ClusterConfig[Cluster].Port+")/"+ClusterConfig[Cluster].Name)
}

type STREAM struct {
	File           string              `json:"filename,sql"`
	Loadlabel      base.LOADLABEL      `json:"loadlabel"`
	LoadProperties base.LOADPROPERTIES `json:"loadproperties"`
	Properties     base.PROPERTIES     `json:"properties"`
	StandardClient base.STANDARDCLIENT `json:"StandardClient"`
}

func GetCurrentTime() string {
	return "msyql_" + strconv.FormatInt(time.Now().UnixNano()/1e6, 10)
}

//hive local
func (this *STREAM) Local_load(ImportData io.Reader, url string, User string, Pwd string, SubmitImportTaskResult *(base.SubmitImportTask)) error {
	req, err := http.NewRequest("PUT", url, ImportData)
	if err != nil {
		return err
	}
	req.Header.Set("strict_mode", "false")
	req.Header.Set("label", this.Loadlabel.LabelName)
	if this.LoadProperties.ColumnSeparator != "" && this.LoadProperties.ColumnSeparator != "\t" {
		req.Header.Set("column_separator", this.LoadProperties.ColumnSeparator)
	}
	if this.LoadProperties.Columns != "" {
		req.Header.Set("columns", this.LoadProperties.Columns)
	}
	if this.LoadProperties.Where != "" {
		req.Header.Set("where", this.LoadProperties.Where)
	}
	if this.Properties.MaxFilterRatio != "" {
		req.Header.Set("max_filter_ratio", this.Properties.MaxFilterRatio)
	}
	if this.LoadProperties.Partitions != "" {
		req.Header.Set("partitions", this.LoadProperties.Partitions)
	}
	req.Header.Set("Expect", "100-continue")
	req.SetBasicAuth(User, Pwd)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	log.Println(string(body))
	err = json.Unmarshal(body, SubmitImportTaskResult)
	if err != nil {
		return err
	}
	return nil
}

//hive local
func (this *STREAM) Hive_load(Username string, url string, User string, Pwd string, SubmitImportTaskResult *(base.SubmitImportTask)) error {
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("label", this.Loadlabel.LabelName)

	req.Header.Set("columnSeparator", "\t")
	log.Println("erp: " + Username)
	req.Header.Set("erp", Username)
	req.Header.Set("hex", "1")

	if this.LoadProperties.Columns != "" {
		req.Header.Set("columns", this.LoadProperties.Columns)
	}
	if this.LoadProperties.Where != "" {
		req.Header.Set("where", this.LoadProperties.Where)
	}
	if this.Properties.MaxFilterRatio != "" {
		req.Header.Set("maxFilterRatio", this.Properties.MaxFilterRatio)
	}
	if this.LoadProperties.Partitions != "" {
		req.Header.Set("partitions", this.LoadProperties.Partitions)
	}
	if this.File != "" {
		req.Header.Set("sql", fmt.Sprintf("%X", this.File))
	}
	if this.StandardClient.Market != "" {
		req.Header.Set("sparkMarket", this.StandardClient.Market)
	}
	if this.StandardClient.Business_line != "" {
		req.Header.Set("sparkBusiness_line", this.StandardClient.Business_line) //user
	} else {
		return errors.New("Business_line is null")
	}
	if this.StandardClient.Production != "" {
		req.Header.Set("sparkProduction", this.StandardClient.Production)
	} else {
		return errors.New("Production is null")
	}
	if this.StandardClient.Queue != "" {
		req.Header.Set("sparkQueue", this.StandardClient.Queue)
	} else {
		return errors.New("sparkQueue is  null")
	}
	if this.StandardClient.HadoopUserCertificate != "" {
		req.Header.Set("Hadoop_user_certificate", this.StandardClient.HadoopUserCertificate)
	} else {
		return errors.New("HadoopUserCertificate is null")
	}
	log.Print(req.Header)

	req.Header.Set("Expect", "100-continue")
	req.SetBasicAuth(User, Pwd)
	log.Println(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	log.Println(string(body))
	return nil
}

func GetUserName(re *base.GetListResult, Verify string) string {
	client := &http.Client{}
	current_milli_time := strconv.FormatInt(time.Now().UnixNano()/1e6, 10)
	sign := md5.New()
	sign.Write([]byte(base.VerifyToken + current_milli_time + Verify))

	req, err := http.NewRequest("POST", base.VerifyUrl+"?ticket="+Verify+"&url="+base.VerifyUrl+"&ip=0.0.0.0&app="+
		base.VerifyApp+"&time="+current_milli_time+"&token="+base.VerifyToken+"&sign="+hex.EncodeToString(sign.Sum(nil)), nil)
	if err != nil {
		re.Message = err.Error()
		return ""
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		re.Message = err.Error()
		return ""
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		re.Message = err.Error()
		return ""
	}
	var VertifyListResut = base.VertifyListResut{}

	err = json.Unmarshal(body, &VertifyListResut)
	if err != nil {
		re.Message = err.Error()
		return ""
	}

	return VertifyListResut.REQ_DATA.Username
}

func WriteString(LoadString *strings.Builder, key string, value string, bcomma bool) {
	if bcomma {
		LoadString.WriteString(",")
	}
	LoadString.WriteString("\n\"" + key + "\" = \"" + value + "\"")
}

func GetUsernameBysso(r *http.Request) base.GetListResult {
	me := base.GetListResult{
		BaseHttpResult: base.BaseHttpResult{
			Code:    1,
			Message: "failed",
		},
	}
	if *Username == "" {
		cookie, err := r.Cookie("")
		if err != nil {
			me.Message = "Get Cookie err!"
			return me
		}
		*Username = GetUserName(&me, cookie.Value)
	}
	me.Code = http.StatusOK
	me.Message = "Success"
	return me
}

func GetStandardClient(r *http.Request, StandardClient *base.STANDARDCLIENT) error {
	var re base.GetListResult
	re = GetUsernameBysso(r)
	if re.Message != "Success" {
		return errors.New(re.Message)
	}
	//get clusterCode by martName
	//Get martuser and cluster
	me := GetMarketList(&re, *Username, StandardClient.Market, false)
	if me == nil {
		re.Message = re.Message
		return errors.New("get Market failed!")
	}
	//Get ProductionAccount Buiness
	pe := GetProductionList(&re, *Username, me.Clustercode, me.Martcode, StandardClient.Market, StandardClient.Production, false)
	if re.Message != "Success" {
		return errors.New("get Production failed! " + re.Message)
	}

	martCode := GetMartBusinessLineDetail(&re, pe.BuinessLine, pe.Martname)
	if re.Message != "Success" {
		return errors.New("get MartBusinessLineDetail failed! " + re.Message)
	}

	hadoopUserCertificate := GetHadoopUserCertificate(&re, pe.BuinessLine, pe.ProductionAccountCode)
	if re.Message != "Success" {
		return errors.New("get HadoopUserCertificate failed! " + re.Message)
	}
	log.Println(*Username, me.Clustercode, me.Martcode, StandardClient.Market, StandardClient.Production, pe.ProductionAccountCode, pe.BuinessLine, StandardClient.Queue)
	HiveLoad := GetHiveLoadPar(&re, *Username, me.Clustercode, me.Martcode, StandardClient.Market, StandardClient.Production, pe.ProductionAccountCode, pe.BuinessLine, StandardClient.Queue)

	if re.Message != "Success" {
		return errors.New("Cannot get SPark par! " + re.Message)
	}
	StandardClient.Market = HiveLoad.ClusterCode
	StandardClient.Business_line = HiveLoad.Business_line
	StandardClient.Production = martCode
	StandardClient.Queue = StandardClient.Queue[5:]
	StandardClient.HadoopUserCertificate = hadoopUserCertificate
	log.Println("martet: " + StandardClient.Market)
	log.Println("business_line: " + StandardClient.Business_line)
	log.Println("production: " + StandardClient.Production)
	log.Println("queue: " + StandardClient.Queue)
	log.Println("certificate: " + StandardClient.HadoopUserCertificate)
	return nil
}
