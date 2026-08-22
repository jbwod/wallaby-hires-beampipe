# Local CASDA credential template

`casda.ini` is a template only. Never add real usernames, passwords, tokens, or
signed URLs to this directory.

1. Copy the template to a path outside the repository and outside any shared graph
   fixture directory.
2. Populate it through the deployment's approved secret-delivery mechanism.
3. Restrict it before use:

   ```bash
   chmod 600 /secure/path/casda.ini
   ```

4. Prefer short-lived staged HTTPS URLs and omit `inputs.credentials_ini_url` when
   the run does not require a credentials file.
5. Apply the deployment retention policy to the generated DALiuGE
   `inputs/casda.ini` after the execution and incident-debugging window.

OPAL account registration is available at
[opal.atnf.csiro.au/register](https://opal.atnf.csiro.au/register).
