node {
  stage('SCM') {
    git 'ssh://git@code.eghuro.com:222/alex/dcat-dry.git'
  }
  stage('SonarQube analysis') {
    def scannerHome = tool name: 'SonarQubeScanner', type: 'hudson.plugins.sonar.SonarRunnerInstallation';
    withSonarQubeEnv('sonar') {
      sh "${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=DCAT-DRY"
    }
  }
}