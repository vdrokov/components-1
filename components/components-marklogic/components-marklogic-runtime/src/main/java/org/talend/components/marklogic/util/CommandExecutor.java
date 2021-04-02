//==============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
//==============================================================================

package org.talend.components.marklogic.util;

import java.io.IOException;
import java.util.List;

/**
 * Util class used for mock system package.
 */
public class CommandExecutor {

    /**
     * Creates and execute system command line process
     *
     * @param command String to execute in console
     * @return Process which was executed
     * @throws IOException when execution exception occurs
     */
    public static Process executeCommand(List<String> commandList) throws IOException {
        ProcessBuilder pb;
        if (System.getProperty("os.name").toLowerCase().startsWith("windows")) {
            pb = new ProcessBuilder("cmd", "/c");
        } else {
            pb = new ProcessBuilder("sh");
        }

        pb.command(commandList);
        return pb.start();
    }
}
