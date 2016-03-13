# Introduction #

Now that the original paper is complete, there are different places to take the library, things to finish, and the like.  This wiki page is here as a gathering place for ideas of what to do with the library.


# Next Steps #

Basic (obvious) next steps:
  * Clean up the code.
  * Refactor data classes into a graph-oriented structure.
  * Submit and investigate bugs.
  * Write API for reading the entire link graph into memory.
  * Re-approach edge cases in parsing.

The completion of these steps will finish the polishing of the original project, corresponding to a [1.0 release](http://code.google.com/p/wikipedia-map-reduce/wiki/1_0_ReleaseReqs).  Where to take it from here?  [ProjectGoals](http://code.google.com/p/wikipedia-map-reduce/wiki/ProjectGoals) discusses this larger design question.


Larger, feature-based next steps need to help meet the goals below, as enumerated in [ProjectGoals](http://code.google.com/p/wikipedia-map-reduce/wiki/ProjectGoals).

  1. to offer a library which parses Wikipedia at the revision-text level;
  1. to create a platform which can be easily used with M/R to perform analyses; and
  1. to generally simplify the process of procuring analysis-friendly Wikipedia data.

'Big idea' tasks:
  * Finish polishing the original project, as in the basic steps listed above.
  * Document the code, parsing process and analytical process.
  * Revisit the manner in which analysis can be performed.
  * Determine the ways in which analysis is most often performed.
  * Explore ways of simplifying the data-parsing flow.

Implementations of these 'big ideas' will happen in later releases.