//  temporary for form actions

import { redirect } from '@sveltejs/kit';

export const actions = {
	default: async ({ request }) => {
		// [TODO] delete after testing
		const data = await request.formData();
		redirect(303, '/results');
		return { success: true };
	}
};
