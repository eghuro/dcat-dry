node {
  stage('SCM') {
    git credentialsId: 'fd96f917-d9f2-404d-8797-2078859754ef', url: 'ssh://git@code.eghuro.com:222/alex/dcat-dry.git'
  }
  stage('Build environment') {
	withPythonEnv('python3') {
	    sh 'python3 -m pip install --upgrade pip'
		sh 'pip install --use-feature=fast-deps --use-deprecated=legacy-resolver -r requirements.txt'
		sh 'pip check'
	}
  }
  stage('SonarQube analysis') {
    def scannerHome = tool name: 'SonarQubeScanner', type: 'hudson.plugins.sonar.SonarRunnerInstallation';
    withSonarQubeEnv('sonar') {
      sh "${scannerHome}/bin/sonar-scanner -Dsonar.projectKey=DCAT-DRY"
    }
  }
}