/*
 * MIT License
 *
 * Copyright (c) 2021 Grégory Van den Borre
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package be.yidizgames.module.database.hsqldb;

import be.yildizgames.module.database.hsqldb.query.HsqldbQueryBuilder;
import be.yildizgames.module.database.query.TableSchema;
import be.yildizgames.module.database.query.TableSchemaColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.UUID;

/**
 * @author Grégory Van den Borre
 */
class HsqdbQueryBuilderTest {

    @Nested
    class Select {

        @Test
        void happyFlow() {
            var c1 = TableSchemaColumn.integer("test").notNull();
            var builder = new HsqldbQueryBuilder(TableSchema.createWithoutId("SelectHF", c1));
            Assertions.assertEquals("SELECT test, t2 FROM SelectHF;", builder.select().columns(c1, TableSchemaColumn.varchar("t2", 5)).build());
        }

        @Test
        void uuid() {
            var c1 = TableSchemaColumn.uuid("test").notNull();
            var builder = new HsqldbQueryBuilder(TableSchema.createWithoutId("SelectHF", c1));
            Assertions.assertEquals("SELECT test, t2 FROM SelectHF;", builder.select().columns(c1, TableSchemaColumn.varchar("t2", 5)).build());
        }
    }

    @Nested
    class Insert {
        @Test
        void happyFlow() {
            var c1 = TableSchemaColumn.integer("c1").notNull();
            var c2 = TableSchemaColumn.varchar("c2", 32).notNull();
            var c3 = TableSchemaColumn.uuid("c3").notNull();
            var builder = new HsqldbQueryBuilder(TableSchema.createWithoutId("insertHF", c1, c2, c3));
            var query = builder.insert(c1.is(12), c2.is("test"), c3.is(UUID.randomUUID())).build();
            Assertions.assertEquals("INSERT INTO insertHF (c1, c2, c3) VALUES (?, ?, ?);", query);
        }
    }

    @Nested
    class Merge {
        @Test
        void happyFlow() {
            var c1 = TableSchemaColumn.integer("c1").notNull();
            var c2 = TableSchemaColumn.varchar("c2", 32).notNull();
            var c3 = TableSchemaColumn.uuid("c3").notNull();
            var builder = new HsqldbQueryBuilder(TableSchema.createWithId("insertHF", c1, c2, c3));
            var query = builder.merge(c1.is(12), c2.is("test"), c3.is(UUID.randomUUID())).build();
            Assertions.assertEquals("MERGE INTO insertHF USING (VALUES(?,?,?,?)) AS vals(c1, c1,c2,c3) ON (insertHF.c1 = vals.c1) WHEN MATCHED THEN UPDATE SET insertHF.c1 = vals.c1,insertHF.c2 = vals.c2,insertHF.c3 = vals.c3 WHEN NOT MATCHED THEN INSERT VALUES (vals.c1,vals.c1,vals.c2,vals.c3);", query);
        }
    }
}
