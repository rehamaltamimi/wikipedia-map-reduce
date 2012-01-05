package wikiParser.edges;

import wmr.core.RevisionFingerprinter;

public interface FingerprintingEdgeGenerator extends EdgeGenerator {
    public void setFingerprintingParser(RevisionFingerprinter parser);

}
