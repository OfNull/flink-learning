package flinklearning._11connection;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public final class SocketDynamicTableSource implements ScanTableSource {

    private final String hostname;
    private final int port;
    private final byte byteDelimiter;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final DataType producedDataType;

    public SocketDynamicTableSource(String hostname, int port, byte byteDelimiter, DecodingFormat<DeserializationSchema<RowData>> decodingFormat, DataType producedDataType) {
        this.hostname = hostname;
        this.port = port;
        this.byteDelimiter = byteDelimiter;
        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(runtimeProviderContext, producedDataType);

        SocketSourceFunction sourceFunction = new SocketSourceFunction(hostname, port, byteDelimiter, deserializer);

        return SourceFunctionProvider.of(sourceFunction, false);

    }

    @Override
    public DynamicTableSource copy() {
        return new SocketDynamicTableSource(hostname, port, byteDelimiter, decodingFormat, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "Socket Table Source";
    }
}
