 /*
  * This file is part of the Yildiz-Engine project, licenced under the MIT License  (MIT)
  *
  *  Copyright (c) 2019 Grégory Van den Borre
  *
  *  More infos available: https://engine.yildiz-games.be
  *
  *  Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
  *  documentation files (the "Software"), to deal in the Software without restriction, including without
  *  limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
  *  of the Software, and to permit persons to whom the Software is furnished to do so,
  *  subject to the following conditions:
  *
  *  The above copyright notice and this permission notice shall be included in all copies or substantial
  *  portions of the Software.
  *
  *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
  *  WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
  *  OR COPYRIGHT  HOLDERS BE LIABLE FOR ANY CLAIM,
  *  DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE  SOFTWARE.
  *
  */
 package be.yildizgames.module.database.hsqldb;

 import be.yildizgames.module.database.QueryBuilder;
 import be.yildizgames.module.database.TableSchema;

 import java.util.Arrays;
 import java.util.StringJoiner;
 import java.util.stream.Collectors;

 /**
  * @author Grégory Van den Borre
  */
 public class HsqldbQueryBuilder extends QueryBuilder {

     public HsqldbQueryBuilder(TableSchema table) {
         super(table);
     }

     @Override
     public QueryBuilder selectAllFrom() {
         this.append("SELECT * FROM " + table.getTableName() + " ");
         return this;
     }

     @Override
     public QueryBuilder select(String... columns) {
         var c = Arrays.stream(columns)
                 .collect(Collectors.joining(", "));
         this.append("SELECT " + c + " FROM " + table.getTableName() + " ");
         return this;
     }

     @Override
     public QueryBuilder limit(int number) {
         this.append("fetch first " + number + " rows only ");
         return this;
     }

     @Override
     public QueryBuilder merge(String id, String... columns) {
         this.append("MERGE INTO " + table.getTableName() + " USING (VALUES(" + buildMergeParams(columns.length + 1) + ")) AS vals(" + id + ", " + buildMergeColums(columns) + ") ON (" + table + "." + id + " = vals." + id
                 + ") WHEN MATCHED THEN UPDATE SET " + buildMergeMatched(columns)
                 + " WHEN NOT MATCHED THEN INSERT VALUES (vals." + id + "," + buildMergeNotMatched(columns) + ");");
         return this;
     }

     private String buildMergeParams(int length) {
         var joiner = new StringJoiner(",");
         for (int i = 0; i < length; i++) {
             joiner.add("?");
         }
         return joiner.toString();
     }

     private String buildMergeColums(String... columns) {
         var joiner = new StringJoiner(",");
         for (String column : columns) {
             joiner.add(column);
         }
         return joiner.toString();
     }

     private String buildMergeMatched(String... columns) {
         var joiner = new StringJoiner(",");
         for (String column : columns) {
             joiner.add(table + "." + column + " = vals." + column);
         }
         return joiner.toString();
     }

     private String buildMergeNotMatched(String... columns) {
         var joiner = new StringJoiner(",");
         for (String column : columns) {
             joiner.add("vals." + column);
         }
         return joiner.toString();
     }
 }
