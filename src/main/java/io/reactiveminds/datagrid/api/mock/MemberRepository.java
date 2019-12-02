package io.reactiveminds.datagrid.api.mock;

import org.springframework.data.repository.CrudRepository;

@org.springframework.stereotype.Repository
public interface MemberRepository extends CrudRepository<Member, String> {
	
	
}
