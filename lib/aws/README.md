# AWS Tooling
This module is for interacting with AWS services.

This assumes that you've already configured your AWS credentials. [Here](https://aws.amazon.com/getting-started/guides/setup-environment/module-three/)
and [here](https://docs.aws.amazon.com/cli/latest/userguide/sso-configure-profile-token.html#sso-configure-profile-token-auto-sso) are the instructions for how to do that (these assume that you've already [installed](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) the AWS CLI).

For this project, we are using the [Northwestern AWS login portal](https://www.it.northwestern.edu/support/login/aws.html). Log in, and then under apps, select the app for the project. Then, select "Command line or programmatic access" and follow the instructions in order to set up access.

For SSO access, run `aws sso login --profile [PROFILE NAME]` before accessing, in order to create temporary auth credentials for your session. For convenience, I aliased this for my own computer:
`alias nw_aws_login="aws sso login --profile [PROFILE NAME]"`