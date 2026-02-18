# Ideas for expansion

## Compare mode

Suppose that you have your version running, and a new IET comes out - don't overwrite it, but do the comparisons and make a series of accept/deny click boxes?

## Better non-project handling

There are different types of non-project rows.  I'm not sure what they are, so they weren't carefully considered

## Project aggregation

Our projects are granular - sometimes they will be different years of the same project, or different codes for non-project work.  From a higher level view we want to aggregate some of these.  So we want a way to merge some projects and view at that level (even if the allocations need to be at the more granular level).

## Workflow

The input to this is based on what finance thinks.  We put in changes and later those changes are reflected in finance.  These changes are often in flight when the input is generated.  If we track our submitted changes (RASRs) we could sync our understanding and finance's understanding more accurately, and track which changes we make here are reflected in which rasr requests.

## PI Report

Show effort levels / burn rates across all projects for which a person is a PI.

## Concurrency

The tool is now just a local single user setup.  To make it more of a shared system would take some concurency hancling

## Security

Moving to a multi-user setup would also require at least authentication, and probably a more thoughtful look at the security.

## Scenario planning

The branch system is the core, but how would you extend it to allow for looking at different scenarios?

## Make a proper changelog

At the moment we have the TSVs but they shoudl probably be collected into a database

## "Expected" things

Is there a special category for expected or probable things?  For instance, TIC ends in three months, but we anticipate an extension.  Or we anticipate an NCE on something.  Do we just assume those as True or do we want some kind of a marker? 
