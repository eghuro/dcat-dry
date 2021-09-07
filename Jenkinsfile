node {
  stage('SCM') {
    checkout scm
  }
  stage('SonarQubeScanner') {
    def scannerHome = tool name: ‘SonarQubeScanner’, type: ‘hudson.plugins.sonar.SonarRunnerInstallation’
    withSonarQubeEnv() {
      sh "${scannerHome}/bin/sonar-scanner"
    }
  }
}