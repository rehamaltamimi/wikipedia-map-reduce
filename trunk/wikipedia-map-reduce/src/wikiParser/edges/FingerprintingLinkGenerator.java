package wikiParser.edges;

import wikiParser.RevisionFingerprinter;

public interface FingerprintingLinkGenerator extends LinkGenerator {
    public void setFingerprintingParser(RevisionFingerprinter parser);

}
