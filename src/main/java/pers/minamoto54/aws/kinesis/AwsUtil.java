package pers.minamoto54.aws.kinesis;

import com.amazonaws.ClientConfiguration;

public class AwsUtil {

    public static ClientConfiguration setProxySettingsFromSystemProperties(final ClientConfiguration clientConfiguration) {

        final String proxyHost = System.getProperty("http.proxyHost");
        if (proxyHost != null) {
            clientConfiguration.setProxyHost(proxyHost);
        }

        final String proxyPort = System.getProperty("http.proxyPort");
        if (proxyPort != null) {
            clientConfiguration.setProxyPort(Integer.parseInt(proxyPort));
        }

        final String proxyUser = System.getProperty("http.proxyUser");
        if (proxyUser != null) {
            clientConfiguration.setProxyUsername(proxyUser);
        }

        final String proxyPassword = System.getProperty("http.proxyPassword");
        if (proxyPassword != null) {
            clientConfiguration.setProxyPassword(proxyPassword);
        }

        final String proxyDomain = System.getProperty("http.auth.ntlm.domain");
        if (proxyDomain != null) {
            clientConfiguration.setProxyDomain(proxyDomain);
        }

        final String workstation = System.getenv("COMPUTERNAME");
        if (proxyDomain != null && workstation != null) {
            clientConfiguration.setProxyWorkstation(workstation);
        }

        return clientConfiguration;
    }

}
