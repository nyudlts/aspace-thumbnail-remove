package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/nyudlts/go-aspace"
	"log"
	"os"
	"regexp"
	"strings"
)

var (
	client          *aspace.ASClient
	config          string
	environment     string
	test            bool
	aoPtn           = regexp.MustCompile("/repositories/7/archival_objects/[0-9]+")
	iFile           string
	dosRemovedCount int
	skipped         int = 0
	logName         string
	repositoryID    int
	resourceID      int
)

type DORef struct {
	URI   string
	Index int
}

const scriptVersion = "v1.0.1"

func init() {
	flag.StringVar(&config, "config", "", "")
	flag.StringVar(&environment, "env", "", "")
	flag.BoolVar(&test, "test", false, "")
	flag.StringVar(&logName, "log", "thumbnail-destroyer.log", "")
	flag.IntVar(&repositoryID, "repository", 0, "")
	flag.IntVar(&resourceID, "resource", 0, "")
}

func setClient() {
	//create an ASClient
	var err error
	client, err = aspace.NewClient(config, environment, 20)
	if err != nil {
		log.Printf("[ERROR] %s", err.Error())
		os.Exit(1)
	}

	log.Printf("[INFO] client created for %s", client.RootURL)
	fmt.Printf("[INFO] client created for %s\n", client.RootURL)
}

func main() {
	flag.Parse()
	logFile, err := os.Create(logName)
	if err != nil {
		panic(err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	log.Printf("[INFO] running `vlp-destroyer` %s", scriptVersion)
	fmt.Printf("[INFO] running `vlp-destroyer` %s\n", scriptVersion)

	setClient()

	if test {
		log.Printf("[INFO] running in test mode, no dos will be unlinked or deleted")
		fmt.Printf("[INFO] running in test mode, no dos will be unlinked or deleted\n")
	}
	aos, err := getAOs()
	if err != nil {
		panic(err)
	}

	fmt.Println(aos)

	dosRemovedCount = 0
	for _, aoUri := range aos {
		log.Printf("[INFO] checking %s for handles", aoUri)
		fmt.Printf("[INFO] checking %s for handles\n", aoUri)
		repoId, aoID, err := aspace.URISplit(aoUri)
		if err != nil {
			log.Printf("[ERROR] %s", err.Error())
			continue
		}

		ao, err := client.GetArchivalObject(repoId, aoID)
		if err != nil {
			log.Printf("[ERROR] %s", err.Error())
			continue
		}

		DORefs := []DORef{}

		//iterate through the instances
		for i, instance := range ao.Instances {
			//check if the instance is a digital object
			if instance.InstanceType == "digital_object" {
				//iterate through the digital object map
				for _, doURI := range instance.DigitalObject {
					//check for aeon link objects
					res, err := hasThumbnails(doURI)
					if err != nil {
						log.Printf("[ERROR] %s", err.Error())
						continue
					}
					if res {
						DORefs = append(DORefs, DORef{URI: doURI, Index: i})
					}
				}
			}
		}

		//if there are no DOs to remove, continue to next ao
		if len(DORefs) < 1 {
			log.Printf("[INFO] no Aeon Links found in %s", aoUri)
			continue
		}

		for _, doRef := range DORefs {
			//unlink the DO from the AO
			log.Printf("[INFO] unlinking of do %s from ao %s", doRef.URI, ao.URI)
			if test == true {
				log.Printf("[INFO] test-mode -- skipping unlinking of do %s from ao %s", doRef.URI, ao.URI)
			} else {
				msg, err := unlinkDO(repoId, aoID, ao, doRef.Index)
				if err != nil {
					log.Printf(fmt.Sprintf("[ERROR] %s", err.Error()))
					continue
				}
				log.Printf(fmt.Sprintf("[INFO] %s", msg))
			}

			//delete the DO
			log.Printf("[INFO] deleting %s", doRef.URI)
			if test == true {
				log.Printf("[INFO] test-mode -- skipping delete of %s\n", doRef.URI)
				skipped = skipped + 1
				continue
			} else {
				msg, err := deleteDO(doRef.URI)
				if err != nil {
					log.Printf("[ERROR] %s", err.Error())
					continue
				}
				log.Printf("[INFO] %s", *msg)
			}

			dosRemovedCount = dosRemovedCount + 1
		}

	}
	log.Printf("[INFO] remove-aeon-links complete, %d digital objects unlinked and removed, %d skipped", dosRemovedCount, skipped)
	fmt.Printf("[INFO] remove-aeon-links complete, %d digital objects unlinked and removed, %d skipped\n", dosRemovedCount, skipped)

}

func getAOs() ([]string, error) {
	aos := []string{}

	tree, err := client.GetResourceTree(repositoryID, resourceID)
	if err != nil {
		return aos, err
	}

	jBytes, err := json.Marshal(tree)
	if err != nil {
		return aos, err
	}

	aoss := aoPtn.FindAll(jBytes, -1)

	for _, ao := range aoss {
		aos = append(aos, string(ao))
	}

	return aos, nil

}

func unlinkDO(repoID int, aoID int, ao aspace.ArchivalObject, ii int) (string, error) {
	//remove the instance from instance slice
	oLength := len(ao.Instances)
	ao.Instances = append(ao.Instances[:ii], ao.Instances[ii+1:]...)
	nLength := len(ao.Instances)

	//check that the instance was removed
	if nLength != oLength-1 {
		return "", fmt.Errorf("%d is not equal to %d -1", nLength, oLength)
	}

	msg, err := client.UpdateArchivalObject(repoID, aoID, ao)
	if err != nil {
		return "", err
	}

	return msg, nil
}

func deleteDO(doURI string) (*string, error) {
	repoID, doID, err := aspace.URISplit(doURI)
	if err != nil {
		return nil, err
	}

	do, err := client.GetDigitalObject(repoID, doID)
	if err != nil {
		return nil, err
	}

	if test != true {
		msg, err := client.DeleteDigitalObject(repoID, doID)
		if err != nil {
			return nil, err
		}
		msg = strings.ReplaceAll(msg, "\n", "")
		msg = fmt.Sprintf("%s {\"file-uri\":\"%s\",\"title\":\"%s\"}", msg, do.URI, do.Title)
		return &msg, nil
	} else {
		msg := fmt.Sprintf("test-mode, skipping deletion of %s, file-uri: %s, title: %s", doURI, do.FileVersions[0].FileURI, do.Title)
		return &msg, nil
	}

}

// check that a digital object only has 1 fileversion and that it contains an aeon link
func hasThumbnails(doURI string) (bool, error) {
	repoID, doID, err := aspace.URISplit(doURI)
	if err != nil {
		return false, err
	}

	do, err := client.GetDigitalObject(repoID, doID)
	if err != nil {
		return false, err
	}

	if len(do.FileVersions) == 1 {
		if do.FileVersions[0].UseStatement == "Image-Thumbnail" {
			return true, nil
		}
		return false, nil
	}

	return false, nil
}
