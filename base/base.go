package base

type Task struct {
	//kafka and hdfs
	Id                   int64  `json:"id,jobid"`
	State                string `json:"state"`
	Progress             string `json:"progress"`         //routine load offset msg
	CreateTime           string `json:"createTime"`       //Stream Load PrePareTime
	ErrorLogUrls         string `json:"errorLogUrls,url"` //sql.NullString
	ImportType           string `json:"importtype"`       //hdfs kafka
	Cluster              string `json:"cluster"`          //ModificationStates
	Database             string `json:"database"`
	Name                 string `json:"name"`
	PauseTime            string `json:"pauseTime"`
	EndTime              string `json:"endTime"`
	TableName            string `json:"tableName"`
	DataSourceType       string `json:"dataSourceType"`
	CurrentTaskNum       int32  `json:"currentTaskNum"`
	JobProperties        string `json:"jobProperties"`
	DataSourceProperties string `json:"dataSourceProperties"`
	CustomProperties     string `json:"customProperties"`
	Statistic            string `json:"statistic"`
	ReasonOfStateChanged string `json:"reasonOfStateChanged"`
	Label                string `json:"Label"`
	Type                 string `json:"type"`
	EtlInfo              string `json:"etlinfo"`
	TaskInfo             string `json:"taskinfo"`
	ErrorMsg             string `json:"errormsg"`
	EtlStartTime         string `json:"etlstarttime"`
	EtlFinishTime        string `json:"etlfinishtime"`
	LoadStartTime        string `json:"loadstarttime"`
	LoadFinishTime       string `json:"loadfinishtime"`
	TransactionId        int    `json:"TransactionId"`
	Coordinator          string `json:"Coordinator"`
	TransactionStatus    string `json:"TransactionStatus"`
	LoadJobSourceType    string `json:"LoadJobSourceType"`
	CommitTime           string `json:"CommitTime"`
	FinishTime           string `json:"FinishTime"`
	ErrorReplicasCount   int    `json:"ErrorReplicasCount"`
	ListenerId           int    `json:"ListenerId"`
	TimeoutMs            int    `json:"TimeoutMs"`
}

//ImportTask List
type SubmitImportTask struct {
	Txnid                int    `json:"TxnId"`
	Label                string `json:"Label"`
	Status               string `json:"Status"`
	Message              string `json:"Message"`
	NumberTotalRows      int    `json:"NumberTotalRows"`
	NumberLoadedRows     int    `json:"NumberLoadedRows"`
	NumberFilteredRows   int    `json:"NumberFilteredRows"`
	NumberUnselectedRows int    `json:"NumberUnselectedRows"`
	LoadBytes            int    `json:"LoadBytes"`
	LoadTimeMs           int    `json:"LoadTimeMs"`
	ErrorURL             string `json:"ErrorURL"`
	SparkJobId           string `json:"sparkJobId"`
}

//Code
const (
	CLUSTE5R  = 0
	DATABASE  = 1
	TABLE     = 2
	PARTITION = 3
	START     = 4
	SUSPEND   = 5
	STOP      = 6
)

type OLAP struct {
	ImportType string      `json:"ImportType"`
	Data       interface{} `json:"Data"`
}

//olap database
type LOADLABEL struct {
	Cluster   string `json:"cluster"`
	Database  string `json:"database"`
	Table     string `json:"table"`
	LabelName string `json:"labelname"`
}

//olap database
type LISTDEFINE struct {
	Index    int `json:"index"`
	PageSize int `json:"pagesize"`
	ToTal    int `json:"total"`
}

//job Properties
type PROPERTIES struct {
	Timeout        string `json:"timeout"`
	MaxFilterRatio string `json:"maxfilterratio"`
}

type LOADPROPERTIES struct {
	Columns         string `json:"columns"`
	ColumnSeparator string `json:"columnseparator"`
	Where           string `json:"where,set"`
	Partitions      string `json:"partitions"`
	Set             string `json:"set"`
}

//标准客户端
type STANDARDCLIENT struct {
	Market                string `json:"Market"`
	Production            string `json:"Production"`
	Queue                 string `json:"Queue"`
	Business_line         string `json:"Business_line"`
	HadoopUserCertificate string `json:"HadoopUserCertificate"`
}

//Modification and base
type BaseHttpResult struct {
	Code     int32  `json:"code"`
	Message  string `json:"message,omitempty"`
	Msg      string `json:"msg,omitempty"`
	Pagenum  int    `json:"pagenum,omitempty"`
	Pagesize int    `json:"pagesize,omitempty"`
	Pages    int    `json:"pages,omitempty"`
	Total    int    `json:"total,omitempty"`
	Status   string `json:"status,omitempty"`
	version  string `json:"version"`
}

//GetList
type GetListResult struct {
	BaseHttpResult
	Date interface{} `json:"data"`
}

//Cluster
type ClusterListResult struct {
	Clusters []*string `json:"cluster"`
}

//Database
type DatabaseListResult struct {
	Cluster   string    `json:"cluster"`
	Databases []*string `json:"databases"`
}

//Database
type TableListResult struct {
	Cluster  string    `json:"cluster"`
	Database string    `json:"database"`
	Tables   []*string `json:"tables"`
}

//PartitionsList
type PartiotionsListResult struct {
	Cluster    string    `json:"cluster"`
	Database   string    `json:"database"`
	Table      string    `json:"table"`
	Partitions []*string `json:"partitions"`
}

//ImportTask List
type ImportTaskListResult struct {
	BaseHttpResult
	Total int     `json:"total"`
	Data  []*Task `json:"data"`
}

//ImportTask List
type RoutineLoadResult struct {
	BaseHttpResult
	Routineload string `json:"RoutineLoad"`
}

//SubmitImportTask List
type SubmitImportTaskResult struct {
	BaseHttpResult
	Data SubmitImportTask `json:"data"`
}

type MODIFICATION struct {
	Cluster    string `json:"Cluster"`
	Database   string `json:"Database"`
	ImportType string `json:"ImportType"`
	Action     string `json:"Action"`
	Label      string `json:"Label"`
}

type UgDapMarketListResult struct {
	BaseHttpResult
	List []*MARKETLIST `json:"list"`
}
type MARKETLIST struct {
	Martprincipal string `json:"martprincipal"`
	Martcode      string `json:"martcode"`
	Clustercode   string `json:"clustercode"`
	Martname      string `json:"martname"`
}

type UgDapProductionListResult struct {
	BaseHttpResult
	List []*PRODUCTIONLIST `json:"list"`
}
type PRODUCTIONLIST struct {
	MartUser               string `json:"martuser"`
	ProductionAccountCode  string `json:"productionaccountCode"`
	BuinessLine            string `json:"business_line"`
	ClusterCode            string `json:"clustercode"`
	IsPersonal             string `json:"ispersonal"`
	ProductionAccountAlias string `json:"productionAccountalias"`
}
type UgDapQueueListResult struct {
	BaseHttpResult
	List []*QUEUELIST `json:"list"`
}
type UgMartBusinessLineDetail struct {
	BaseHttpResult
	*MartBusinessLineDetail `json:"data"`
}

type MartBusinessLineDetail struct {
	physicalClusterCode     string `json:"physicalClusterCode"`
	MartCode                string `json:"martCode"`
	authPhysicalClusterCode string `json:"authPhysicalClusterCode"`
	isVirtualMarket         string `json:"isVirtualMarket"`
	businessLine            string `json:"businessLine"`
	isMerged                string `json:"isMerged"`
	parentVirtualCluster    string `json:"parentVirtualCluster"`
	parentVirtualMarket     string `json:"parentVirtualMarket"`
}

type UgQueryUsersDetail struct {
	BaseHttpResult
	QueryUsersDetails []*QueryUsersDetail `json:"users"`
}
type QueryUsersDetail struct {
	group  string `json:"group"`
	market string `json:"market"`
	role   string `json:"role"`
	Token  string `json:"token"`
	user   string `json:"user"`
}

type QUEUELIST struct {
	Business_line string `json:"business_line"`
	ClusterCode   string `json:"clusterCode"`
	Engine        string `json:"engine"`
	MartUser      string `json:"martUser"`
	QueueCode     string `json:"queueCode"`
}

type Tasks []*Task

func (s Tasks) Len() int {
	return len(s)
}
func (s Tasks) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type ByCreateTIme struct {
	Tasks
}

func (s ByCreateTIme) Less(i, j int) bool {
	return s.Tasks[j].CreateTime < s.Tasks[i].CreateTime
}

//to GetListResult Data
type MarketList struct {
	Clustercode string `json:"clustercode"`
	Martcode    string `json:"martcode"`
	MarketListResult
}

type MarketListResult struct {
	Martname []*string `json:"martname"`
}

type Productions struct {
	ProductionAccountCode string `json:"productionaccountCode"`
	BuinessLine           string `json:"business_line"`
	ProductionListResult
}

type ProductionListResult struct {
	Martname       string    `json:"martname"`
	Martproduction []*string `json:"martproduction"`
}

type QueueListResult struct {
	Martname       string    `json:"martname"`
	Martproduction string    `json:"martproduction"`
	Martqueue      []*string `json:"martqueue"`
}

type ReqData struct {
	Username  string      `json:"username"`
	Email     string      `json:"email"`
	Mobile    string      `json:"mobile"`
	Orgid     interface{} `json:"orgid"`
	Personid  string      `json:"personid"`
	Fullname  string      `json:"fullname"`
	Hrmdeptid interface{} `json:"hrmdeptid"`
	Hrmorgid  interface{} `json:"hrmorgid"`
	Orgmame   string      `json:"orgmame"`
	Userid    interface{} `json:"userid"`
}

type VertifyListResut struct {
	REQ_CODE int     `json:"req_code"`
	REQ_FLAG bool    `json:"req_flag"`
	REQ_MSG  string  `json:"req_msg"`
	REQ_DATA ReqData `json:"req_data"`
}

type MySql struct {
	Name string `json:"Name"`
	User string `json:"User"`
	Pwd  string `json:"Pwd"`
	Host string `json:"Host"`
	Port string `json:"Port"`
}

type Hdfs struct {
	Nameservices string      `json:"nameservices"`
	Namenodes    [2]NameNode `json:"namenodes"`
}
type NameNode struct {
	Name        string `json:"name"`
	Rpc_address string `json:"rpcaddress"`
}

type PartitionConfig struct {
	HistoryDays       []*PartitionsHistory `json:"historydays"`
	PartitionClusters []*PartitionCluster  `json:"partitionclusters"`
}

type PartitionsHistory struct {
	StartDay int `json:"startday"`
	EndDay   int `json:"endday"`
}

type PartitionCluster struct {
	PartitionClustername string               `json:"clustername"`
	PartitionDatabases   []*PartitionDatabase `json:"databases"`
}

type PartitionDatabase struct {
	PartitionDatabasename string            `json:"databasename"`
	PartitionTables       []*PartitionTable `json:"tables"`
}

type PartitionTable struct {
	PartitionTablename      string   `json:"tablename"`
	PartitionCreateBy       string   `json:"partition_create_by"`
	PartitionHeadNameBy     string   `json:"partition_head_name_by"`
	PartitionStartOffsetDay int      `json:"start_offset_day"`
	PartitionEndOffsetDay   int      `json:"end_offset_day"`
	Ppartitions             []string `json:"partition"`
}

var (
	TOKEN                     = ""
	APPID                     = "api"
	GETMARKETLISTURL          = "api"
	GETPRODUCTIONLISTURL      = "api"
	GETQUEUELISTLISTURL       = "api"
	GetMartBusinessLineDetail = "api"
	GetQueryUsersURL          = "api"
	VerifyUrl                 = "api"
	VerifyApp                 = "msyql"
	VerifyToken               = "token"
)
