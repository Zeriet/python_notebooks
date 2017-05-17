package com.ge.current.em.aggregation.request;

import com.ge.current.em.analytics.dto.SiteReadingDTO;

import java.util.Optional;

/**
 * Created by 212582112 on 3/9/17.
 */
public class SiteReadingDTOContainer {
    private boolean isValidSiteReading;
    private SiteReadingDTO siteReadingDTO;
    private Optional<String> rawSiteReading;
    private Optional<String> errorMsg;

    public SiteReadingDTOContainer(boolean isValidSiteReading,
                                   SiteReadingDTO siteReadingDTO,
                                   Optional<String> rawSiteReading,
                                   Optional<String> errorMsg) {
        this.isValidSiteReading = isValidSiteReading;
        this.siteReadingDTO = siteReadingDTO;
        this.rawSiteReading = rawSiteReading;
        this.errorMsg = errorMsg;
    }

    public boolean isValidSiteReading() {
        return isValidSiteReading;
    }

    public SiteReadingDTO getSiteReadingDTO() {
        return siteReadingDTO;
    }

    public Optional<String> getRawSiteReading() {
        return rawSiteReading;
    }

    public Optional<String> getErrorMsg() {
        return errorMsg;
    }

    @Override
    public String toString() {
        return "SiteReadingDTOContainer{" +
                "isValidSiteReading=" + isValidSiteReading +
                ", siteReadingDTO=" + siteReadingDTO +
                ", rawSiteReading=" + rawSiteReading +
                ", errorMsg=" + errorMsg +
                '}';
    }

    public static final class SiteReadingDTOContainerBuilder {
        private boolean isValidSiteReading;
        private SiteReadingDTO siteReadingDTO;
        private Optional<String> rawSiteReading;
        private Optional<String> errorMsg;

        public static SiteReadingDTOContainerBuilder aSiteReadingDTOContainer() {
            return new SiteReadingDTOContainerBuilder();
        }

        public SiteReadingDTOContainerBuilder withInvalidReading() {
            this.isValidSiteReading = false;
            return this;
        }

        public SiteReadingDTOContainerBuilder withValidReading() {
            this.isValidSiteReading = true;
            return this;
        }

        public SiteReadingDTOContainerBuilder withSiteReading(SiteReadingDTO siteReadingDTO) {
            this.siteReadingDTO = siteReadingDTO;
            return this;
        }

        public SiteReadingDTOContainerBuilder withRawSiteReading(String rawSiteReading) {
            this.rawSiteReading = Optional.of(rawSiteReading);
            return this;
        }

        public SiteReadingDTOContainerBuilder withErrorMessage(String errorMsg) {
            this.errorMsg = Optional.of(errorMsg);
            return this;
        }

        public SiteReadingDTOContainer build() {
            return new SiteReadingDTOContainer(isValidSiteReading, siteReadingDTO, rawSiteReading, errorMsg);
        }
    }
}
