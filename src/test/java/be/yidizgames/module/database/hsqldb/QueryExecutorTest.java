/*
 * MIT License
 *
 * Copyright (c) 2019 Grégory Van den Borre
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package be.yidizgames.module.database.hsqldb;

import be.yildizgames.module.database.DatabaseConnectionProviderFactory;
import be.yildizgames.module.database.DbProperties;
import be.yildizgames.module.database.QueryExecutor;
import be.yildizgames.module.database.TableSchema;
import be.yildizgames.module.database.TableSchemaColumn;
import be.yildizgames.module.database.hsqldb.HsqldbSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.UUID;

/**
 * @author Grégory Van den Borre
 */
class QueryExecutorTest {

    @Nested
    class CreateDatabase {

        @Test
        void withUuid() {
            HsqldbSystem.support();
            var provider = DatabaseConnectionProviderFactory.getInstance().create(new DbProperties() {
                @Override
                public String getDbUser() {
                    return "sa";
                }

                @Override
                public int getDbPort() {
                    return 0;
                }

                @Override
                public String getDbPassword() {
                    return "sa";
                }

                @Override
                public String getDbHost() {
                    return "localhost";
                }

                @Override
                public String getDbName() {
                    return "test";
                }

                @Override
                public String getSystem() {
                    return HsqldbSystem.KEY;
                }

                @Override
                public String getDbRootUser() {
                    return "sa";
                }

                @Override
                public String getDbRootPassword() {
                    return "sa";
                }
            });
            var executor = new QueryExecutor(provider);
            executor.createTableIfNotExists(TableSchema.createWithoutId("test", TableSchemaColumn.uuid("uuid")));
            var uuid = UUID.randomUUID();
            try(var c = provider.getConnection(); var p = c.prepareStatement("insert into test values(?)")) {
                p.setObject(1, uuid);
                p.execute();
            } catch (SQLException throwables) {
                throw new IllegalStateException(throwables);
            }
            try(var c = provider.getConnection(); var p = c.prepareStatement("select * from test")) {
                var r = p.executeQuery();
                r.next();
                Assertions.assertEquals(uuid.toString(), r.getObject(1).toString());
            } catch (SQLException throwables) {
                throw new IllegalStateException(throwables);
            }
            executor.dropTables("test");
        }

    }
}
