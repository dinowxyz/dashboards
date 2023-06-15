import grpc
import ibc_proto.ibc as ibc_pb2
import ibc_proto.ibc_grpc as ibc_pb2_grpc

def decode_ibc_tokens(row):
    channel = grpc.insecure_channel('cosmos-grpc.polkachu.com:14990')
    stub = ibc_pb2_grpc.QueryStub(channel)

    # Check if the asset in assets.0 is an IBC token
    asset_0 = row["assets.0"]
    if isinstance(asset_0, str) and asset_0.startswith("ibc/"):
        # Decode the IBC token and update the DataFrame
        request = ibc_pb2.QueryIBCDenomRequest(denom=asset_0)
        response = stub.IBCDenom(request)
        symbol = response.denom.display_name
        row["assets.0"] = symbol
    
    # Check if the asset in assets.1 is an IBC token
    asset_1 = row["assets.1"]
    if isinstance(asset_1, str) and asset_1.startswith("ibc/"):
        # Decode the IBC token and update the DataFrame
        request = ibc_pb2.QueryIBCDenomRequest(denom=asset_1)
        response = stub.IBCDenom(request)
        symbol = response.denom.display_name
        row["assets.1"] = symbol
    
    channel.close()
    return row
