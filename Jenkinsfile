node {
  stage('SCM') {
    checkout scm
  }
  stage('SonarQubeScanner') {
    def scannerHome = tool 'SonarScanner';
    withSonarQubeEnv() {
      sh "${scannerHome}/bin/sonar-scanner"
    }
  }
}