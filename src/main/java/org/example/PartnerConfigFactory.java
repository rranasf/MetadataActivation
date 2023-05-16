package org.example;

public class PartnerConfigFactory {
    PartnerConfigReader configReader = new PartnerConfigReader();

    public PartnerConfig getPartnerConfig(String partnerName) {
        if (partnerName.equalsIgnoreCase("testPartner")) {
            PartnerConfig partnerConfig = configReader.loadPartnerConfig("testPartnerConfig.json");
            return partnerConfig;
        }
        else {
            return null;
        }
    }
}
