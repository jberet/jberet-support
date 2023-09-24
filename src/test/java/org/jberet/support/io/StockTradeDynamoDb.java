package org.jberet.support.io;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbIgnore;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * {@link StockTrade} implementation enriched with DynamoDB mapping annotations.
 */
@DynamoDbBean
public class StockTradeDynamoDb extends StockTrade {
    @DynamoDbIgnore
    @Override
    public Date getDate() {
        return super.getDate();
    }

    @DynamoDbPartitionKey
    public String getDateKey() {
        return getDateFormat().format(getDate());
    }

    public void setDateKey(String s) {
        try {
            setDate(getDateFormat().parse(s));
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid date " + s);
        }
    }

    private static DateFormat getDateFormat() {
        return new SimpleDateFormat("MM/dd/yyyy");
    }

    @DynamoDbSortKey
    @Override
    public String getTime() {
        return super.getTime();
    }

    public static StockTradeDynamoDb parseCsv(String csvLine) {
        String[] cells = csvLine.split(",", 7);
        StockTradeDynamoDb item = new StockTradeDynamoDb();
        item.setDateKey(cells[0]);
        item.setTime(cells[1]);
        item.setOpen(Double.parseDouble(cells[2]));
        item.setHigh(Double.parseDouble(cells[3]));
        item.setLow(Double.parseDouble(cells[4]));
        item.setClose(Double.parseDouble(cells[5]));
        item.setVolume(Double.parseDouble(cells[6]));
        return item;
    }
}
