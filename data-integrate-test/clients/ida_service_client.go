package clients

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	
	// 需要先编译proto文件生成go代码
	// pb "data-integrate-test/proto/mirapb"
)

// IDAServiceClient IDA服务客户端
type IDAServiceClient struct {
	conn   *grpc.ClientConn
	// client pb.MiraIdaAccessClient
}

func NewIDAServiceClient(host string, port int) (*IDAServiceClient, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	
	return &IDAServiceClient{
		conn: conn,
		// client: pb.NewMiraIdaAccessClient(conn),
	}, nil
}

func (c *IDAServiceClient) Close() error {
	return c.conn.Close()
}

// CreateDataSourceRequest 创建数据源请求（简化版本，用于测试）
type CreateDataSourceRequest struct {
	Name         string
	Host         string
	Port         int32
	DBType       int32 // 1.mysql, 2.kingbase, 3.gbase, 4.vastbase
	Username     string
	Password     string
	DatabaseName string
}

// CreateDataSourceResponse 创建数据源响应
type CreateDataSourceResponse struct {
	DataSourceId int32
	Success      bool
	Message      string
}

// CreateAssetRequest 创建资产请求（简化版本，用于测试）
type CreateAssetRequest struct {
	AssetName    string
	AssetEnName  string
	DataSourceId int32
	DBName       string
	TableName    string
}

// CreateAssetResponse 创建资产响应
type CreateAssetResponse struct {
	AssetId  int32
	Success  bool
	Message  string
}

// CreateDataSource 创建数据源
func (c *IDAServiceClient) CreateDataSource(ctx context.Context, req *CreateDataSourceRequest) (*CreateDataSourceResponse, error) {
	// TODO: 实现CreateDataSource调用（需要proto编译）
	// 当前返回mock数据，实际应该调用gRPC服务
	// protoReq := &pb.CreateDataSourceRequest{
	//     BaseRequest: &pb.BaseRequest{
	//         RequestId: fmt.Sprintf("ds_%d", time.Now().Unix()),
	//     },
	//     Name: req.Name,
	//     Host: req.Host,
	//     Port: req.Port,
	//     DbType: req.DBType,
	//     Username: req.Username,
	//     Password: req.Password,
	//     InstanceName: req.DatabaseName,
	// }
	// resp, err := c.client.CreateDataSource(ctx, protoReq)
	
	// 临时实现：返回mock数据
	return &CreateDataSourceResponse{
		DataSourceId: 1000, // Mock ID
		Success:      true,
		Message:      "Mock: DataSource created successfully",
	}, nil
}

// CreateAsset 创建资产
func (c *IDAServiceClient) CreateAsset(ctx context.Context, req *CreateAssetRequest) (*CreateAssetResponse, error) {
	// TODO: 实现CreateAsset调用（需要proto编译）
	// 当前返回mock数据，实际应该调用gRPC服务
	// protoReq := &pb.CreateAssetRequest{
	//     BaseRequest: &pb.BaseRequest{
	//         RequestId: fmt.Sprintf("asset_%d", time.Now().Unix()),
	//     },
	//     AssetName: req.AssetName,
	//     AssetEnName: req.AssetEnName,
	//     DataSourceId: req.DataSourceId,
	//     DbName: req.DBName,
	//     TableName: req.TableName,
	// }
	// resp, err := c.client.CreateAsset(ctx, protoReq)
	
	// 临时实现：返回mock数据
	return &CreateAssetResponse{
		AssetId: 2000, // Mock ID
		Success: true,
		Message: "Mock: Asset created successfully",
	}, nil
}

// GetPrivateAssetInfoByEnName 获取资产信息
func (c *IDAServiceClient) GetPrivateAssetInfoByEnName(ctx context.Context, assetEnName string) (interface{}, error) {
	// TODO: 实现GetPrivateAssetInfoByEnName调用
	// req := &pb.GetPrivateAssetInfoByEnNameRequest{
	//     BaseRequest: &pb.BaseRequest{
	//         RequestId: fmt.Sprintf("test_%d", time.Now().Unix()),
	//     },
	//     AssetEnName: assetEnName,
	// }
	// return c.client.GetPrivateAssetInfoByEnName(ctx, req)
	return nil, fmt.Errorf("not implemented: need proto compilation")
}

