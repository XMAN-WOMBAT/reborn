// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/ngaut/zkhelper"
	"github.com/reborndb/go/errors2"
	"github.com/reborndb/reborn/pkg/models"
	"github.com/reborndb/reborn/pkg/utils"
)

type MigrateTaskInfo struct {
	FromSlot   int    `json:"from"`
	ToSlot     int    `json:"to"`
	NewGroupId int    `json:"new_group"`
	Delay      int    `json:"delay"`
	CreateAt   string `json:"create_at"`
	Percent    int    `json:"percent"`
	Status     string `json:"status"`
	Id         string `json:"id"`
}

type SlotMigrateProgress struct {
	SlotId    int `json:"slot_id"`
	FromGroup int `json:"from"`
	ToGroup   int `json:"to"`
	Remain    int `json:"remain"`
}

func (p SlotMigrateProgress) String() string {
	return fmt.Sprintf("migrate Slot: slot_%d From: group_%d To: group_%d remain: %d keys", p.SlotId, p.FromGroup, p.ToGroup, p.Remain)
}

type MigrateTask struct {
	MigrateTaskInfo
	stopChan     chan struct{}
	coordConn    zkhelper.Conn
	productName  string
	slotMigrator SlotMigrator
	progressChan chan SlotMigrateProgress
}

func NewMigrateTask(info MigrateTaskInfo) *MigrateTask {
	return &MigrateTask{
		MigrateTaskInfo: info,
		slotMigrator:    &RebornSlotMigrator{},
		stopChan:        make(chan struct{}),
		productName:     globalEnv.ProductName(),
	}
}

func (t *MigrateTask) migrateSingleSlot(slotId int, to int) error {
	// set slot status
	s, err := models.GetSlot(t.coordConn, t.productName, slotId)
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}
	if s.State.Status != models.SLOT_STATUS_ONLINE && s.State.Status != models.SLOT_STATUS_MIGRATE {
		log.Warning("status is not online && migrate", s)
		return nil
	}

	from := s.GroupId
	if s.State.Status == models.SLOT_STATUS_MIGRATE {
		from = s.State.MigrateStatus.From
	}

	// cannot migrate to itself, just ignore
	if from == to {
		log.Warning("from == to, ignore", s)
		return nil
	}

	// make sure from group & target group exists
	exists, err := models.GroupExists(t.coordConn, t.productName, from)
	if err != nil {
		return errors.Trace(err)
	}
	if !exists {
		log.Errorf("src group %d not exist when migrate from %d to %d", from, from, to)
		return errors.NotFoundf("group %d", from)
	}

	exists, err = models.GroupExists(t.coordConn, t.productName, to)
	if err != nil {
		return errors.Trace(err)
	}
	if !exists {
		return errors.NotFoundf("group %d", to)
	}

/***************************************************************/
	// make sure to group has at least one master server ---zjp 20160711

	log.Warning("Get group %d has no master master not found")
	groupTo, err :=models.GetGroup(t.coordConn, t.productName, to)
	if err != nil {
		log.Warning(err)
		 return errors.Trace(err)
	}
	log.Warning("Get to master ")
	toMaster, err := groupTo.Master(t.coordConn)
	if err != nil {
		log.Warning(err)
		return errors.Trace(err)
	}

	if toMaster == nil {
		log.Warning("to master not found")
		return errors.NotFoundf("group %d has no master", to)
	}
	log.Warning("Get to master  != nil")

	var perr error
	for i := 0; i < haMaxRetryNum; i++ {
		if perr = utils.Ping(toMaster.Addr, globalEnv.StoreAuth()); perr == nil {
			break
		}
		
		perr = errors.Trace(perr)
		time.Sleep(time.Duration(haRetryDelay) * time.Second)
	}

	if perr != nil{
		log.Warning(perr)
		log.Warning("To master is not online")
		return errors.Trace(perr)
	}
	groupFrom, err :=models.GetGroup(t.coordConn, t.productName, from)
	if err != nil {
		log.Warning(err)
		 return errors.Trace(err)
	}
	log.Warning("Get from master ")
	fromMaster, err := groupFrom.Master(t.coordConn)
	if err != nil {
		log.Warning(err)
		return errors.Trace(err)
	}

	if fromMaster == nil {
		log.Warning(" from master not found")
		return errors.NotFoundf("group %d has no master", to)
	}
	log.Warning("Get from master  != nil")

	var Fmerr error
	for i := 0; i < haMaxRetryNum; i++ {
		if Fmerr = utils.Ping(fromMaster.Addr, globalEnv.StoreAuth()); Fmerr == nil {
			break
		}
		
		Fmerr = errors.Trace(Fmerr)
		time.Sleep(time.Duration(haRetryDelay) * time.Second)
	}

	if Fmerr != nil{
		log.Warning(Fmerr)
		log.Warning("From master is not online")
		return errors.Trace(Fmerr)
	}
	groupFrom, gerr :=models.GetGroup(t.coordConn, t.productName, from)
	if gerr != nil {
		log.Warning(gerr)
		 return errors.Trace(gerr)
	}


	/***************************************************************/

	// modify slot status
	if err := s.SetMigrateStatus(t.coordConn, from, to); err != nil {
		log.Error(err)
		return errors.Trace(err)
	}

	err = t.slotMigrator.Migrate(s, from, to, t, func(p SlotMigrateProgress) {
		// on migrate slot progress
		if p.Remain%500 == 0 {
			log.Info(p)
		}
	})
	if err != nil {
		log.Error(err)
		return errors.Trace(err)
	}

	// migrate done, change slot status back
	s.State.Status = models.SLOT_STATUS_ONLINE
	s.State.MigrateStatus.From = models.INVALID_ID
	s.State.MigrateStatus.To = models.INVALID_ID
	if err := s.Update(t.coordConn); err != nil {
		log.Error(err)
		return errors.Trace(err)
	}

	return nil
}

func (t *MigrateTask) stop() error {
	if t.Status == MIGRATE_TASK_MIGRATING {
		t.stopChan <- struct{}{}
	}
	return nil
}

// migrate multi slots
func (t *MigrateTask) run() error {
	// create zk conn on demand
	t.coordConn = CreateCoordConn()
	defer t.coordConn.Close()

	to := t.NewGroupId
	t.Status = MIGRATE_TASK_MIGRATING
	for slotId := t.FromSlot; slotId <= t.ToSlot; slotId++ {
		err := t.migrateSingleSlot(slotId, to)
		if errors2.ErrorEqual(err, ErrStopMigrateByUser) {
			log.Info("stop migration job by user")
			break
		} else if err != nil {
			log.Error(err)
			t.Status = MIGRATE_TASK_ERR
			return errors.Trace(err)
		}
		t.Percent = (slotId - t.FromSlot + 1) * 100 / (t.ToSlot - t.FromSlot + 1)
		log.Info("total percent:", t.Percent)
	}
	t.Status = MIGRATE_TASK_FINISHED
	log.Info("migration finished")
	return nil
}

func preMigrateCheck(t *MigrateTask) (bool, error) {
	conn := CreateCoordConn()
	defer conn.Close()

	slots, err := models.GetMigratingSlots(conn, t.productName)

	if err != nil {
		return false, errors.Trace(err)
	}
	// check if there is migrating slot
	if len(slots) > 1 {
		return false, errors.New("more than one slots are migrating, unknown error")
	}
	if len(slots) == 1 {
		slot := slots[0]
		if t.NewGroupId != slot.State.MigrateStatus.To || t.FromSlot != slot.Id || t.ToSlot != slot.Id {
			return false, errors.Errorf("there is a migrating slot %+v, finish it first", slot)
		}
	}
	return true, nil
}
