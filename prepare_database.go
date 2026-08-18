package embeddedpostgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	fmtCloseDBConn = "unable to close database connection: %w"
	fmtAfterError  = "%v happened after error: %w"
)

type initDatabase func(binaryExtractLocation, runtimePath, pgDataDir, username, password, locale string, encoding string, logger *os.File) error
type createDatabase func(port uint32, username, password, database string) error

func defaultInitDatabase(binaryExtractLocation, runtimePath, pgDataDir, username, password, locale string, encoding string, logger *os.File) error {
	passwordFile, err := createPasswordFile(runtimePath, password)
	if err != nil {
		return err
	}

	args := []string{
		"-A", "password",
		"-U", username,
		"-D", pgDataDir,
		fmt.Sprintf("--pwfile=%s", passwordFile),
	}

	if locale != "" {
		args = append(args, fmt.Sprintf("--locale=%s", locale))
	}

	if encoding != "" {
		args = append(args, fmt.Sprintf("--encoding=%s", encoding))
	}

	postgresInitDBBinary := filepath.Join(binaryExtractLocation, "bin/initdb")
	postgresInitDBProcess := exec.Command(postgresInitDBBinary, args...)
	postgresInitDBProcess.Stderr = logger
	postgresInitDBProcess.Stdout = logger

	if err = postgresInitDBProcess.Run(); err != nil {
		logContent, readLogsErr := readLogsOrTimeout(logger) // we want to preserve the original error
		if readLogsErr != nil {
			logContent = []byte(string(logContent) + " - " + readLogsErr.Error())
		}
		return fmt.Errorf("unable to init database using '%s': %w\n%s", postgresInitDBProcess.String(), err, string(logContent))
	}

	if err = os.Remove(passwordFile); err != nil {
		return fmt.Errorf("unable to remove password file '%v': %w", passwordFile, err)
	}

	return nil
}

func createPasswordFile(runtimePath, password string) (string, error) {
	passwordFileLocation := filepath.Join(runtimePath, "pwfile")
	if err := os.WriteFile(passwordFileLocation, []byte(password), 0600); err != nil {
		return "", fmt.Errorf("unable to write password file to %s", passwordFileLocation)
	}

	return passwordFileLocation, nil
}

func defaultCreateDatabase(port uint32, username, password, database string) (err error) {
	if database == "postgres" {
		return nil
	}

	db, err := openDatabaseConnection(port, username, password, "postgres")
	if err != nil {
		return errorCustomDatabase(database, err)
	}

	defer func() {
		err = connectionClose(db, err)
	}()

	if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE \"%s\"", database)); err != nil {
		return errorCustomDatabase(database, err)
	}

	return nil
}

// connectionClose closes the database connection and handles the error of the function that used the database connection
func connectionClose(db io.Closer, err error) error {
	closeErr := db.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf(fmtCloseDBConn, closeErr)

		if err != nil {
			err = fmt.Errorf(fmtAfterError, closeErr, err)
		} else {
			err = closeErr
		}
	}

	return err
}

func healthCheckDatabaseOrTimeout(config Config) error {
	healthCheckSignal := make(chan bool)

	defer close(healthCheckSignal)

	timeout, cancelFunc := context.WithTimeout(context.Background(), config.startTimeout)

	defer cancelFunc()

	go func() {
		for timeout.Err() == nil {
			if err := healthCheckDatabase(config.port, config.database, config.username, config.password); err != nil {
				continue
			}
			healthCheckSignal <- true

			break
		}
	}()

	select {
	case <-healthCheckSignal:
		return nil
	case <-timeout.Done():
		return errors.New("timed out waiting for database to become available")
	}
}

func healthCheckDatabase(port uint32, database, username, password string) (err error) {
	db, err := openDatabaseConnection(port, username, password, database)
	if err != nil {
		return err
	}

	defer func() {
		err = connectionClose(db, err)
	}()

	if _, err := db.Query("SELECT 1"); err != nil {
		return err
	}

	return nil
}

// openDatabaseConnection opens a connection over the pgx stdlib driver.
//
// This library previously used github.com/lib/pq here. That module is in
// maintenance mode and, as of 2026, carries five advisories with no upstream
// fix (GO-2026-6166, -6168, -6170, -6171, -6172). Importing it registered its
// driver in every binary that linked this package, so those advisories were
// reachable through database/sql's runtime driver lookup even for callers that
// never opened a "postgres" connection.
func openDatabaseConnection(port uint32, username string, password string, database string) (*sql.DB, error) {
	return sql.Open("pgx", fmt.Sprintf("host=localhost port=%d user=%s password=%s dbname=%s sslmode=disable",
		port,
		username,
		password,
		database))
}

func errorCustomDatabase(database string, err error) error {
	return fmt.Errorf("unable to connect to create database with custom name %s with the following error: %s", database, err)
}
