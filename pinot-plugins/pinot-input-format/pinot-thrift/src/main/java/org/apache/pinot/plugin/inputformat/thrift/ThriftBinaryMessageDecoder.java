package org.apache.pinot.plugin.inputformat.thrift;

import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class ThriftBinaryMessageDecoder implements StreamMessageDecoder<byte[]> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThriftBinaryMessageDecoder.class);

    private ThriftRecordExtractor _recordExtractor;
    private Class<?> _thriftClass;
    private Map<String, Integer> _fieldIds = new HashMap<>();

    private TBase _tObject;

    private TDeserializer tBinaryDeserializer;

    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName) throws Exception {

        try {
            _thriftClass = this.getClass().getClassLoader().loadClass(props.get("THRIFT_CLASS_NAME"));
            tBinaryDeserializer = new TDeserializer();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Map<? extends TFieldIdEnum, org.apache.thrift.meta_data.FieldMetaData> metaDataMap =
                FieldMetaData.getStructMetaDataMap(_tObject.getClass());
        for (TFieldIdEnum tFieldIdEnum : metaDataMap.keySet()) {
            _fieldIds.put(tFieldIdEnum.getFieldName(), Short.toUnsignedInt(tFieldIdEnum.getThriftFieldId()));
        }

        ThriftRecordExtractorConfig recordExtractorConfig = new ThriftRecordExtractorConfig();
        recordExtractorConfig.setFieldIds(_fieldIds);
        _recordExtractor = new ThriftRecordExtractor();
        _recordExtractor.init(fieldsToRead, recordExtractorConfig);

    }

    @Nullable
    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
        return decode(payload, 0, payload.length, destination);
    }

    @Nullable
    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
        try {
            tBinaryDeserializer.deserialize(_tObject, payload);
            return _recordExtractor.extract(_tObject, destination);

        } catch (TException e) {
            LOGGER.error("Error while deserializing thrift binary message ", e);
        }
        return null;
    }
}
