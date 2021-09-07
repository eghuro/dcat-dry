node {
  stage('SCM') {
    checkout scm
  }
  stage('SonarQubeScanner') {
    def scannerHome = tool ‘SonarQubeScanner’, type: ‘hudson.plugins.sonar.SonarRunnerInstallation’
    withSonarQubeEnv() {
      sh "${scannerHome}/bin/sonar-scanner"
    }
  }
}