package wikiParser.edges;

import wikiParser.RevisionFingerprinter;

public interface FingerprintingEdgeGenerator extends EdgeGenerator {
    public void setFingerprintingParser(RevisionFingerprinter parser);

}
