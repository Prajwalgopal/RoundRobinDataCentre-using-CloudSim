package com.datacentre.impl;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

/**This broker allocates VMs to data center following the
 * <a href="http://en.wikipedia.org/wiki/Round-robin_scheduling">Round-robin</a> algorithm.
 * @author Ameya Adhyapak https://github.com/ameya01
 **/

public class RRDataCenterBroker extends DatacenterBroker  
{

    /**
     * Creates an instance of this class associating to it a given name.
     * @param name The name to be associated to this broker. It might not be <code>null</code> or empty.
     * @throws Exception If the name contains spaces.
     */
    public RRDataCenterBroker(String name) throws Exception 
    {
        super(name);
    }

    @Override
    protected void processResourceCharacteristics(SimEvent ev) 
    {        
        DatacenterCharacteristics characteristics = (DatacenterCharacteristics) ev.getData();
        getDatacenterCharacteristicsList().put(characteristics.getId(), characteristics);

        if (getDatacenterCharacteristicsList().size() == getDatacenterIdsList().size()) 
        {
        	distributeReqForNewVmsAcrossDatacentersUsingRR();
        }
    }

    /**
     * Distributes the VMs across the data centers using the round-robin approach. A VM is allocated to a data center only if there isn't  
     * a VM in the data center with the same id.     
     */
    protected void distributeReqForNewVmsAcrossDatacentersUsingRR()
	{
		// TODO Auto-generated method stub
		int numOfVmsAllocated = 0;
		int i= 0;
		
		final List<Integer> availableDatacenters= getDatacenterIdsList();
		
		for (Vm vm : getVmList())
		{
			int datacenterID = availableDatacenters.get(i++ % availableDatacenters.size());
			String datacenterName = CloudSim.getEntityName(datacenterID);
			
			if (!getVmsToDatacentersMap().containsKey(vm.getId()))
			{
				Log.printLine(CloudSim.clock() + ":" + getName() + ": Trying to Create VM #" + vm.getId() + "in" + datacenterName);
				sendNow(datacenterID, CloudSimTags.VM_CREATE_ACK,vm);
				numOfVmsAllocated++;
			}
		}
		
		setVmsRequested(numOfVmsAllocated);
		setVmsAcks(0);
	}

}