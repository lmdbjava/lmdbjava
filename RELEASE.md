# Releasing

This page explains how to perform a release, a general overview of the sequence,
how to immediately manage any credential compromise, and how to replace the
existing credentials with new ones.

### Performing a Release

To perform a release, a project maintainer can run
`mvn -Prelease release:clean release:prepare`. This will update the POMs to a
formal release version number, Git tag, and increment the version number for
ongoing development. Travis will perform the actual release.

### Release Background

At the end of the Travis build, the presence of a Git tag will cause Travis to
use `mvn deploy`. The `mvn deploy` command deploys to the project's BinTray
repository. The `mvn deploy` uses the Git-hosted `.settings.xml`, which in
turn refers to the BinTray username and API key from the Travis CI environment.

The Travis CI environment declares the `BINTRAY_USER` in plain text, as this is
already public information. It also declares the `BINTRAY_PASS`, but this is
included as a Travis encrypted variable. It is never made available to pull
request (PR) builds.

Travis next decrypts the `secrets.tar.enc`. Inside the resulting `secrets.tar`
is two JSON files, both of which are used when executing HTTP POST methods
against the BinTray REST API. It is critical these files are not displayed (eg
do not `cat` them to the console or similar), as doing so will necessitate the
Security Action Plan shown below.

Travis executes a BinTray REST call to GPG sign the version. The GPG signature
is embedded in one of the JSON files contained in the `secrets.tar` file.

Finally, Travis runs a BinTray REST call to push the version to Maven Central
and close the release. Maven Central will validate the uploads and eventually
index and mirror them.

### Security Action Plan

If the `BINTRAY_PASS` is compromised, the risk is limited to misuse of the
API key (it does not allow login to the BinTray UI). To revoke the API key,
login to BinTray, then select Your Profile,
[Edit](https://bintray.com/profile/edit), API Key, Revoke It.

If the `secrets.tar`-hosted `mvn-central-sync.json` is compromised, the risk is
limited to misuse of the API key (it does not allow login to the Sonatype OSS or
Jira instance). To revoke the API key, login to
[OSS SonaType](https://oss.sonatype.org/) then in the top right-hand corner
select the username, Profile, User Token, Reset User Token.

If the `secrets.tar`-hosted `gpg-sign.json` is compromised, the risk is limited
to signing untrusted artifacts. The GPG key cannot be used to login anywhere,
but the GPG key should be revoked as soon as possible. Use
`gpg --list-secret-keys` to find the key ID then `gpg --edit-key THE_ID`,
`revuid`, `save`, `quit` then `gpg --send-keys THE_ID`.

### New Credential Setup

It is assumed the user has installed the
[Travis CLI](https://github.com/travis-ci/travis.rb#readme) tool on their
machine and will run it from the project's working directory. This is needed for
the correct Travis keys to be used when encrypting variables and/or files.

Any BinTray user with administrative access to the repository can use their
API key. To change the effective BinTray user:

1. Set the`BINTRAY_USER` in `.travis.yml` to the new BinTray username
2. Delete the existing `.travis.yml` `env` `secure:` line (which is the BinTray
   password)
3. Use `travis encrypt BINTRAY_PASS=the_api_key` and update `.travis.yml`

Any OSS Sonatype user with deploy rights to the Maven Central group ID can be
used for the Maven Central sync operation. Login to OSS Sonatype and generate
an API key (username, Profile, User Token, Create User Token). Then create a
`mvn-central-sync.json` file in the following form:

``` json
{
  "username": "token-exactly-as-displayed",
  "password": "password-exactly-as-displayed",
  "close": "1"
}
```

Any GPG key can be used for the signing process, however Maven Central requires
the GPG key use the same email address as the OSS Sonatype Jira account which
has group ID deployment permission. The steps to use a new GPG key are:

1. Generate a key (matching the Jira account email address) via `gpg --gen-key`
2. Find the key ID (`gpg --list-secret-keys`)
3. Publish the public key `gpg --send-keys THE_ID`
4. Run `gpg --export -a THE_ID > public.txt`
5. Login to BinTray, then select the organisation, Edit, GPG Signing and replace
   the "Public Key" (there is no need to add any private key here)
6. Run `gpg --export-secret-keys -a THE_ID | awk -vRS='\n' -vORS='\\r\\n' '1' > key.txt`
7. Create `gpg-sign.json` file with the following format (4 lines total):

``` json
{
  "passphrase": "gpg password used when creating the key",
  "private_key": "the text EXACTLY as contained in key.txt"
}
```

After the JSON files are created, the `secrets.jar.enc` must be updated:

1. `rm -f key.txt public.txt secrets.tar.enc secrets.tar`
2. `tar cvf secrets.tar *.json`
3. `travis encrypt-file secrets.tar`
4. `git add secrets.tar.enc`

Do not add `secrets.tar` to Git. To make this more difficult to happen by
accident, `.gitignore` includes entries for the `secrets.jar` and JSON files.

### New GitHub Repositories

If a new GitHub repository is set up, three steps are needed:

1. Create a new Travis CI project (out of the scope of this document)
2. Create a newly-encrypted `secret.tar.enc` and the `secure:` variable in
   `.travis.yml` (use the commands described in the previous section)
3. Create a new BinTray package (see below)

The second step is needed because every Travis project is unique and has its
own encryption key. You cannot use a `secrets.jar.enc` or encrypted variable
from another project, as these keys differ.

To setup a new package in BinTray:

1. Login to BinTray
2. Select the organisation
3. Select the `maven` repository
4. Select Import from GitHub
5. In the imported project, click Edit
6. Enter details such as license, issue tracker URL etc
7. Change the name to observe the package naming convention (add a group ID)
8. Run an initial release
9. In the BinTray UI for the now-released package, click "Add to JCenter"

Once approved, the package will appear in JCenter and the Maven Central
deployment configuration (eg as copied from another repository) will work.
