package org.talend.components.jdbc;

import java.lang.reflect.Method;
import java.net.URL;

public final class BundleUtils {
    private static final Object BUNDLE;

    static {
        Object bundle;
        try {
            ClassLoader ld = BundleUtils.class.getClassLoader();
            Class<?> util = ld.loadClass("org.osgi.framework.FrameworkUtil");
            Method getBundle = util.getMethod("getBundle", Class.class);
            bundle = getBundle.invoke((Object)null, BundleUtils.class);
        } catch (Exception exception) {
            bundle = null;
        }

        BUNDLE = bundle;
    }

    public static URL getFile(String path) {
        URL fileUrl;
        try {
            Method getResource = BUNDLE.getClass().getMethod("getResource", String.class);
            fileUrl = (URL)getResource.invoke(BUNDLE, path);
        } catch (Exception exception) {
            fileUrl = null;
        }

        return fileUrl;
    }

    public static boolean inOSGi() {
        return BUNDLE != null;
    }

    private BundleUtils() {
        super();
    }
}