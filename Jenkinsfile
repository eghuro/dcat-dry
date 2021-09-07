node {
  stage('SCM') {
    checkout scm
  }
  stage('SonarQube analysis') {
    def scannerHome = tool name: ‘SonarQubeScanner’, type: ‘hudson.plugins.sonar.SonarRunnerInstallation’;
    withSonarQubeEnv('sonar') {
      sh "${scannerHome}/bin/sonar-scanner"
    }
  }
}