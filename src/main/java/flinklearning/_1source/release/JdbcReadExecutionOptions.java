package flinklearning._1source.release;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.time.LocalDateTime;

@PublicEvolving
public class JdbcReadExecutionOptions implements Serializable {
    //查询开始时间
    private LocalDateTime startDateTime = LocalDateTime.now();
    //时间增长步长  单位分钟
    private int timeStep = 1;
    //查询Sql
    private String querySql;

    public JdbcReadExecutionOptions(LocalDateTime startDateTime, int timeStep, String querySql) {
        this.startDateTime = startDateTime;
        this.timeStep = timeStep;
        this.querySql = querySql;
    }

    public LocalDateTime getStartDateTime() {
        return startDateTime;
    }

    public void setStartDateTime(LocalDateTime startDateTime) {
        this.startDateTime = startDateTime;
    }

    public int getTimeStep() {
        return timeStep;
    }

    public void setTimeStep(int timeStep) {
        this.timeStep = timeStep;
    }

    public String getQuerySql() {
        return querySql;
    }

    public void setQuerySql(String querySql) {
        this.querySql = querySql;
    }

    public static JdbcReadExecutionOptions.Builder builder() {
        return new JdbcReadExecutionOptions.Builder();
    }

    public static JdbcReadExecutionOptions defaults() {
        return builder().build();
    }

    public static final class Builder {
        //查询开始时间
        private LocalDateTime startDateTime = LocalDateTime.now();
        //时间增长步长  单位分钟
        private int timeStep = 1;
        private String querySql;

        public Builder() {
        }

        public JdbcReadExecutionOptions.Builder withStartDateTime(LocalDateTime startDateTime) {
            this.startDateTime = startDateTime;
            return this;
        }

        public JdbcReadExecutionOptions.Builder withTimeStep(int timeStep) {
            this.timeStep = timeStep;
            return this;
        }

        public JdbcReadExecutionOptions.Builder withQuerySql(String querySql) {
            if (StringUtils.isBlank(querySql)) {
                throw new IllegalArgumentException("JdbcReadExecutionOptions param querySql is not null");
            }
            this.querySql = querySql;
            return this;
        }

        public JdbcReadExecutionOptions build() {
            return new JdbcReadExecutionOptions(this.startDateTime, this.timeStep, this.querySql);
        }
    }
}
