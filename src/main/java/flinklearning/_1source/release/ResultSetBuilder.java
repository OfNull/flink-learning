package flinklearning._1source.release;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.function.FunctionWithException;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * ResultSet
 *
 * @param <T>
 */
@PublicEvolving
public interface ResultSetBuilder<T> extends FunctionWithException<ResultSet, T, SQLException>, Serializable {
}
