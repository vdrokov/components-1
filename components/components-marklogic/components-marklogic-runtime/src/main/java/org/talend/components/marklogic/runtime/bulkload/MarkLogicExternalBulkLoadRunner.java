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

package org.talend.components.marklogic.runtime.bulkload;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.marklogic.exceptions.MarkLogicErrorCode;
import org.talend.components.marklogic.exceptions.MarkLogicException;
import org.talend.components.marklogic.tmarklogicbulkload.MarkLogicBulkLoadProperties;
import org.talend.components.marklogic.util.CommandExecutor;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.i18n.I18nMessages;

/**
 * External mlcp loader to preserve previous behavior and do not lead to regressions.
 * Using MarkLogic Content Pump binaries could be obtain from the MarkLogic official site: https://developer.marklogic.com/products/mlcp/
 * By default tMarkLogicBulkLoad doesn't use it, but the mlcp.jar instead
 * @see MarkLogicInternalBulkLoadRunner
 */
public class MarkLogicExternalBulkLoadRunner extends AbstractMarkLogicBulkLoadRunner {

    private static final I18nMessages MESSAGES = GlobalI18N.getI18nMessageProvider()
            .getI18nMessages(MarkLogicExternalBulkLoadRunner.class);

    private transient static final Logger LOGGER = LoggerFactory.getLogger(MarkLogicBulkLoad.class);

    private static final String CMD_CALL = "mlcp.bat";

    private static final String SH_CALL = "mlcp.sh";

    protected MarkLogicExternalBulkLoadRunner(MarkLogicBulkLoadProperties bulkLoadProperties) {
        super(bulkLoadProperties);
    }

    @Override
    public void performBulkLoad() {
        List<String> mlcpCMDCommandParameters = prepareMLCPCommand();
        List<String> mlcpFinalCommand = prepareMLCPCommandCMD(mlcpCMDCommandParameters);
        runBulkLoading(mlcpFinalCommand);
    }

    List<String> prepareMLCPCommandCMD(List<String> mlcpCommandParameters) {
        List<String> fullMLCPCommand = new ArrayList<>();
        fullMLCPCommand.add(prepareMlcpCommandStart(System.getProperty("os.name")));
        fullMLCPCommand.addAll(mlcpCommandParameters);
        return fullMLCPCommand;
    }

    String prepareMlcpCommandStart(String osName) {
        if (osName.toLowerCase().startsWith("windows")) {
            return CMD_CALL;
        } else {
            return SH_CALL;
        }
    }

    @Override
    protected void runBulkLoading(List<String> parameters) {
        LOGGER.debug(MESSAGES.getMessage("messages.debug.command", parameters));
        LOGGER.info(MESSAGES.getMessage("messages.info.startBulkLoad"));
        try {
            Process mlcpProcess = CommandExecutor.executeCommand(parameters);

            try (InputStream normalInput = mlcpProcess.getInputStream();
                    InputStream errorInput = mlcpProcess.getErrorStream()) {

                Thread normalInputReadProcess = getInputReadProcess(normalInput, System.out);
                normalInputReadProcess.start();

                Thread errorInputReadProcess = getInputReadProcess(errorInput, System.err);
                errorInputReadProcess.start();

                mlcpProcess.waitFor();
                normalInputReadProcess.interrupt();
                errorInputReadProcess.interrupt();

                LOGGER.info(MESSAGES.getMessage("messages.info.finishBulkLoad"));
            }

        } catch (Exception e) {
            LOGGER.error(MESSAGES.getMessage("messages.error.exception", e.getMessage()));
            throw new MarkLogicException(new MarkLogicErrorCode(e.getMessage()), e);
        }
    }

    private Thread getInputReadProcess(final InputStream inputStream, final PrintStream outStream) {
        return new Thread() {

            public void run() {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        outStream.println(line);
                    }
                } catch (IOException ioe) {
                    handleIOException(ioe);
                }
            }
        };
    }

    private void handleIOException(IOException ioe) {
        LOGGER.error(MESSAGES.getMessage("messages.error.ioexception", ioe.getMessage()));
        ioe.printStackTrace();
    }
}
