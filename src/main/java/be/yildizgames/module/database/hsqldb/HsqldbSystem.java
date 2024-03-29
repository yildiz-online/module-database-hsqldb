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

import be.yildizgames.module.database.BaseDatabaseSystem;
import be.yildizgames.module.database.DatabaseConnectionProviderFactory;
import be.yildizgames.module.database.DriverProvider;
import org.hsqldb.jdbc.JDBCDriver;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Grégory Van den Borre
 */
public class HsqldbSystem extends BaseDatabaseSystem {

    public static final String KEY = "hsqldb:file";

    private final DriverProvider driver = JDBCDriver::new;

    private HsqldbSystem(String urlParams) {
        super("jdbc:hsqldb:file:{0};" + urlParams);
    }

    public static void support() {
        support("allow_empty_batch=true;hsqldb.write_delay=false;shutdown=true");
    }

    public static void supportCompactShutdown() {
        support("allow_empty_batch=true;hsqldb.write_delay=false");
    }

    public static void support(String urlParams) {
        support(KEY, urlParams);
    }

    /**
     * Set a custom key to be able to run multiple configuration at the same time when using several database
     * @param key Unique key.
     * @param urlParams Associated parameters.
     */
    public static void support(String key, String urlParams) {
        Logger databaseLogger = Logger.getLogger("hsqldb.db");
        databaseLogger.setUseParentHandlers(false);
        databaseLogger.setLevel(Level.WARNING);
        DatabaseConnectionProviderFactory.getInstance().addSystem(key, new HsqldbSystem(urlParams));
    }

    @Override
    public final String getDriver() {
        return "org.hsqldb.jdbc.JDBCDriver";
    }

    @Override
    public final DriverProvider driverProvider() {
        return this.driver;
    }

    @Override
    public final boolean requirePool() {
        return false;
    }
}
