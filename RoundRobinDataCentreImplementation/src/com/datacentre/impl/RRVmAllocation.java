package com.datacentre.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

/**
 * This file allocates VM to Round-Robin allocation policy.
 * 
 * @author nikeshhegde
 * 
 */

public class RRVmAllocation extends org.cloudbus.cloudsim.VmAllocationPolicy {
	
	// List to store the hosts
	private final Map<String, Host> vm_table = new HashMap<String, Host>();
	
	private final HostList hosts;
	
	public RRVmAllocation(List<? extends Host> list) {
		super(list);
		this.hosts = new HostList(list);
	}
	
	@Override
	public boolean allocateHostForVm(Vm vm) {
		if (this.vm_table.containsKey(vm.getUid())) {
			return true;
		}

		boolean vm_allocated = false;

		Host host = this.hosts.next();
		if (host != null) {
			vm_allocated = this.allocateHostForVm(vm, host);
		}

		return vm_allocated;
	}

	@Override
	public boolean allocateHostForVm(Vm vm, Host host) 
	{
		if (host != null && host.vmCreate(vm)) 
		{
			vm_table.put(vm.getUid(), host);
			Log.formatLine("VM #" + vm.getId() + " has been allocated to the host#" + host.getId() + 
					" datacenter #" + host.getDatacenter().getId() + "(" + host.getDatacenter().getName() + ") #", CloudSim.clock());
			return true;
		}
		return false;
	}

	@Override
	public List<Map<String, Object>> optimizeAllocation(List<? extends Vm> vmList) {
		return null;
	}

	@Override
	public void deallocateHostForVm(Vm vm) {
		Host host = this.vm_table.remove(vm.getUid());

		if (host != null) {
			host.vmDestroy(vm);
		}
	}

	@Override
	public Host getHost(Vm vm) {
		return this.vm_table.get(vm.getUid());
	}

	@Override
	public Host getHost(int vmId, int userId) {
		return this.vm_table.get(Vm.getUid(userId, vmId));
	}
	
}
